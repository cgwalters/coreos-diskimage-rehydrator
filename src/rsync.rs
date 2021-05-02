//use rayon::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap, HashMap};
use std::convert::TryInto;

use crate::bupsplit;

const ROLLSUM_BLOB_MAX: usize = 8192 * 4;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub(crate) struct ChunkId {
    pub(crate) crc32: u32,
    pub(crate) len: u32,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct Chunk<'a> {
    pub(crate) start: u64,
    pub(crate) buf: &'a [u8],
}

/// Create a mapping from CRC32 values to byte array chunks that match it.
pub(crate) fn rollsum_chunks_crc32(mut buf: &[u8]) -> HashMap<ChunkId, Vec<Chunk>> {
    let mut ret = HashMap::<ChunkId, Vec<Chunk>>::new();
    let mut done = false;
    let mut start = 0u64;
    while !buf.is_empty() {
        let ofs = if done {
            buf.len()
        } else {
            if let Some(ofs) = bupsplit::bupsplit_find_ofs(buf) {
                ofs
            } else {
                done = true;
                buf.len()
            }
        };
        let ofs = ofs.min(ROLLSUM_BLOB_MAX);
        let sub_buf = &buf[..ofs];
        let mut crc = crc32fast::Hasher::new();
        crc.update(sub_buf);
        let crc = crc.finalize();
        let chunkid = ChunkId {
            crc32: crc,
            len: sub_buf.len().try_into().unwrap(),
        };

        let v = ret.entry(chunkid).or_default();
        v.push(Chunk {
            start,
            buf: sub_buf,
        });
        start += ofs as u64;
        buf = &buf[ofs..]
    }
    ret
}

/// Statistics.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RollsumDeltaStats {
    pub(crate) matched_size: u64,
    pub(crate) unmatched_size: u64,
    pub(crate) crc_miss: u32,
    pub(crate) chunkid_collision: u32,
    pub(crate) src_chunks: u32,
    pub(crate) dest_chunks: u32,
    pub(crate) dest_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Patch {
    chunks: Vec<PatchEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PatchEntry {
    Literal { start: u64, len: u64 },
    Copy { start: u64, len: u64 },
}

/// rsync-style delta between two byte buffers.
#[derive(Debug, Default)]
pub(crate) struct RollsumDelta<'src, 'dest> {
    pub(crate) matches: BTreeMap<u64, Chunk<'src>>,
    pub(crate) unmatched: BTreeMap<u64, Chunk<'dest>>,
    pub(crate) stats: RollsumDeltaStats,
}

/// Compute an rsync-style delta between src and dest.
pub(crate) fn rollsum_delta<'src, 'dest>(
    src: &'src [u8],
    dest: &'dest [u8],
) -> RollsumDelta<'src, 'dest> {
    let mut delta: RollsumDelta = Default::default();
    let src_chunkset = rollsum_chunks_crc32(&src);
    let dest_chunkset = rollsum_chunks_crc32(&dest);

    delta.stats.src_chunks = src_chunkset.len().try_into().unwrap();
    delta.stats.dest_chunks = dest_chunkset.len().try_into().unwrap();
    delta.stats.dest_size = dest.len() as u64;

    // We now have a mapping [CRC32] -> Vec<Chunk> for both source+destination.
    // The goal is to find chunks in the source that we can reuse.
    for (chunkid, dest_chunks) in dest_chunkset.into_iter() {
        for dest_chunk in dest_chunks {
            let mut found = false;
            // Check to see if there are any chunks that match that CRC32+len in the source.
            if let Some(src_chunks) = src_chunkset.get(&chunkid) {
                // Loop over the source and destination chunks that match that CRC32+len
                for src_chunk in src_chunks.iter() {
                    // It's possible that the destination has duplicate
                    // data; for example, uncompressed disk images may have
                    // large runs of zero bytes.  Hence we now look to
                    // see if we already found a match for this range in the
                    // destination.  If so, we're done.  If not, do
                    // a more expensive check to see if the buffer actually matches.
                    match delta.matches.entry(dest_chunk.start) {
                        Entry::Vacant(e) => {
                            let len = src_chunk.buf.len();
                            assert!(len > 0);
                            // Directly compare the buffers.  If they're not equal, then
                            // we obviously can't reuse it.  rsync uses md5 here, but
                            // let's go for maximum reliablity and just bytewise compare.
                            if src_chunk.buf != dest_chunk.buf {
                                delta.stats.chunkid_collision += 1;
                                continue;
                            }
                            e.insert(Chunk {
                                buf: src_chunk.buf,
                                start: src_chunk.start,
                            });
                            delta.stats.matched_size += len as u64;
                            found = true;
                            break;
                        }
                        Entry::Occupied(_) => {
                            panic!("Duplicate destination offset {}", dest_chunk.start);
                        }
                    }
                }
            } else {
                delta.stats.crc_miss += 1;
            }
            if !found {
                let existed = delta.unmatched.insert(dest_chunk.start, dest_chunk);
                delta.stats.unmatched_size += dest_chunk.buf.len() as u64;
                assert!(!existed.is_some());
            }
        }
    }

    delta
}

impl Patch {
    pub(crate) fn new(src: &[u8], dest: &[u8]) -> (Self, RollsumDeltaStats) {
        let delta = rollsum_delta(src, dest);
        let mut buf = Vec::<u8>::new();
        let mut start = 0u64;
        for (entry_start, entry_match) in delta.matches {
            if entry_start != start {
                debug_assert!(start < entry_start);
                buf.extend(&dest[start as usize..entry_start as usize]);
                start = entry_start;
            }
        }
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rollsum() {
        let empty: &[u8] = &[];
        let single = &[42u8];

        let delta = rollsum_delta(empty, empty);
        assert_eq!(delta.matches.len(), 0);
        let delta = rollsum_delta(single, single);
        assert_eq!(delta.matches.len(), 1);
        assert_eq!(
            delta.matches.get(&0).unwrap(),
            &Chunk {
                buf: single,
                start: 0,
            }
        );
        let delta = rollsum_delta(empty, single);
        assert_eq!(delta.matches.len(), 0);
    }
}
