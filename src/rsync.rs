//use rayon::prelude::*;
use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use serde_derive::{Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap, HashMap};
use std::convert::TryInto;
use std::io::Write;

use crate::bupsplit;

const ROLLSUM_BLOB_MAX: usize = 8192 * 4;
const HEADER: &[u8] = b"DELT";

type CRC32 = u32;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub(crate) struct ChunkId {
    pub(crate) crc32: CRC32,
    pub(crate) len: u32,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct Chunk<'a> {
    pub(crate) start: u64,
    pub(crate) buf: &'a [u8],
}

/// Create a mapping from CRC32 values to byte array chunks that match it.
pub(crate) fn rollsum_chunks_crc32(mut buf_iter: &[u8]) -> HashMap<ChunkId, Vec<Chunk>> {
    let mut ret = HashMap::<ChunkId, Vec<Chunk>>::new();
    let mut done = false;
    let mut start = 0u64;
    while !buf_iter.is_empty() {
        let ofs = if done {
            buf_iter.len()
        } else {
            if let Some(ofs) = bupsplit::bupsplit_find_ofs(buf_iter) {
                ofs
            } else {
                done = true;
                buf_iter.len()
            }
        };
        let ofs = ofs.min(ROLLSUM_BLOB_MAX);
        let sub_buf = &buf_iter[..ofs];
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
        buf_iter = &buf_iter[ofs..]
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
    pub(crate) dest_duplicates: u32,
    pub(crate) dest_size: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct Patch {
    chunks: Vec<PatchEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PatchEntry {
    CopyBuf { start: u64, len: u64 },
    CopySource { start: u64, len: u64 },
}

/// rsync-style delta between two byte buffers.
#[derive(Debug, Default)]
pub(crate) struct RollsumDelta<'src, 'dest> {
    pub(crate) matched: BTreeMap<u64, Chunk<'src>>,
    pub(crate) unmatched_chunks: Vec<&'dest [u8]>,
    // Maps from offset into unmatched_chunks
    pub(crate) unmatched: BTreeMap<u64, usize>,
    pub(crate) stats: RollsumDeltaStats,
}

fn dedup_chunks<'dest>(input: Vec<Chunk<'dest>>) -> (Vec<(u64, usize)>, Vec<&'dest [u8]>) {
    let mut unique = Vec::<&[u8]>::new();
    let mut ret = Vec::new();
    for chunk in input {
        let mut found = false;
        for (i, &uniq) in unique.iter().enumerate() {
            if chunk.buf == uniq {
                ret.push((chunk.start, i));
                found = true;
                break;
            }
        }
        if !found {
            unique.push(chunk.buf);
            ret.push((chunk.start, unique.len() - 1));
        }
    }
    (ret, unique)
}

/// Compute an rsync-style delta between src and dest.
pub(crate) fn rollsum_delta<'src, 'dest>(
    src: &'src [u8],
    dest: &'dest [u8],
) -> RollsumDelta<'src, 'dest> {
    let mut delta: RollsumDelta = Default::default();
    let src_chunkset = rollsum_chunks_crc32(&src);
    let mut dest_chunkset = rollsum_chunks_crc32(&dest);

    delta.stats.src_chunks = src_chunkset.len().try_into().unwrap();
    delta.stats.dest_chunks = dest_chunkset.len().try_into().unwrap();
    delta.stats.dest_size = dest.len() as u64;

    // We now have a mapping [CRC32] -> Vec<Chunk> for both source+destination.
    // The goal is to find chunks in the source that we can reuse.  Retain
    // in "dest_chunkset" all unmatched chunks
    dest_chunkset.retain(|chunkid, dest_chunks| {
        dest_chunks.retain(|dest_chunk| {
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
                    match delta.matched.entry(dest_chunk.start) {
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
                            // We found a match, so don't keep this one in the unmatched set
                            return false;
                        }
                        Entry::Occupied(_) => {
                            panic!("Duplicate destination offset {}", dest_chunk.start);
                        }
                    }
                }
            } else {
                delta.stats.crc_miss += 1;
            }
            true
        });
        // If we found matches for all chunks, remove this entry
        !dest_chunks.is_empty()
    });

    for (_, dest_chunks) in dest_chunkset.into_iter() {
        let origlen = dest_chunks.len();
        delta.stats.unmatched_size += dest_chunks.iter().fold(0u64, |acc, chunk| acc + chunk.buf.len() as u64);
        let (dest_chunks, bufs) = dedup_chunks(dest_chunks);
        delta.stats.dest_duplicates += origlen.checked_sub(dest_chunks.len()).unwrap() as u32;
        let offset = delta.unmatched_chunks.len();
        delta.unmatched_chunks.extend(bufs);
        delta
            .unmatched
            .extend(dest_chunks.into_iter().map(|(s, o)| (s, o + offset)));
    }

    delta
}

pub(crate) fn create_patchfile<W: std::io::Write>(
    src: &[u8],
    dest: &[u8],
    out: W,
) -> Result<RollsumDeltaStats> {
    use itertools::{EitherOrBoth, Itertools};
    let delta = rollsum_delta(src, dest);

    let mut out = zstd::Encoder::new(out, 15)?;

    let mut buflen = 0u64;
    let mut chunk_offsets = HashMap::<usize, u64>::new();
    for (i, chunk) in delta.unmatched_chunks.iter().enumerate() {
        chunk_offsets.insert(i, buflen);
        buflen += chunk.len() as u64;
    }

    out.write_all(HEADER)?;
    out.write_u64::<LittleEndian>(buflen)?;

    let mut patch = Patch {
        ..Default::default()
    };

    for e in delta.matched.iter().zip_longest(delta.unmatched.iter()) {
        let e = e.map_left(|(&start, chunk)| {
            (
                start,
                PatchEntry::CopySource {
                    start: chunk.start,
                    len: chunk.buf.len() as u64,
                },
            )
        });
        let e = e.map_right(|(&start, &bufidx)| {
            let len = delta.unmatched_chunks[bufidx].len() as u64;
            let &buf_start = chunk_offsets.get(&bufidx).unwrap();
            let p = PatchEntry::CopyBuf { start: buf_start, len };
            (start, p)
        });
        match e {
            EitherOrBoth::Both(matched, unmatched) => {
                if matched.0 < unmatched.0 {
                    patch.chunks.push(matched.1);
                    patch.chunks.push(unmatched.1);
                } else {
                    patch.chunks.push(unmatched.1);
                    patch.chunks.push(matched.1);
                }
            }
            EitherOrBoth::Left(matched) => {
                patch.chunks.push(matched.1);
            }
            EitherOrBoth::Right(unmatched) => patch.chunks.push(unmatched.1),
        };
    }

    bincode::serialize_into(out, &patch)?;

    Ok(delta.stats)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rollsum() {
        let empty: &[u8] = &[];
        let single = &[42u8];

        let delta = rollsum_delta(empty, empty);
        assert_eq!(delta.matched.len(), 0);
        let delta = rollsum_delta(single, single);
        assert_eq!(delta.matched.len(), 1);
        assert_eq!(
            delta.matched.get(&0).unwrap(),
            &Chunk {
                buf: single,
                start: 0,
            }
        );
        let delta = rollsum_delta(empty, single);
        assert_eq!(delta.matched.len(), 0);
    }
}
