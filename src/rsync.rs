//use rayon::prelude::*;
use std::collections::{btree_map::Entry, BTreeMap, HashMap};
use std::convert::TryInto;

use crate::bupsplit;

const ROLLSUM_BLOB_MAX: usize = 8192 * 4;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Chunk<'a> {
    pub(crate) crc: u32,
    pub(crate) start: u64,
    pub(crate) buf: &'a [u8],
}

pub(crate) fn rollsum_chunks_crc32(mut buf: &[u8]) -> HashMap<u32, Vec<Chunk>> {
    let mut ret = HashMap::<u32, Vec<Chunk>>::new();
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

        let v = ret.entry(crc).or_default();
        v.push(Chunk {
            crc,
            start,
            buf: sub_buf,
        });
        start += ofs as u64;
        buf = &buf[ofs..]
    }
    ret
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RollsumDeltaStats {
    pub(crate) match_size: u64,
    pub(crate) crc_miss: u32,
    pub(crate) crc_len_collision: u32,
    pub(crate) crc_collision: u32,
    pub(crate) src_chunks: u32,
    pub(crate) dest_chunks: u32,
    pub(crate) dest_size: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RollsumDelta<'a> {
    pub(crate) matches: BTreeMap<u64, Match<'a>>,

    pub(crate) stats: RollsumDeltaStats,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct Match<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) src_start: u64,
    pub(crate) dest_start: u64,
}

pub(crate) fn rollsum_delta<'a>(src: &'a [u8], dest: &[u8]) -> RollsumDelta<'a> {
    let mut delta: RollsumDelta = Default::default();
    let src_chunkset = rollsum_chunks_crc32(&src);
    let dest_chunkset = rollsum_chunks_crc32(&dest);

    delta.stats.src_chunks = src_chunkset.len().try_into().unwrap();
    delta.stats.dest_chunks = dest_chunkset.len().try_into().unwrap();
    delta.stats.dest_size = dest.len() as u64;

    for (&crc, dest_chunks) in dest_chunkset.iter() {
        if let Some(src_chunks) = src_chunkset.get(&crc) {
            for src_chunk in src_chunks.iter() {
                for dest_chunk in dest_chunks.iter() {
                    debug_assert_eq!(src_chunk.crc, dest_chunk.crc);

                    // Same crc32 but different length, skip it.
                    if src_chunk.buf.len() != dest_chunk.buf.len() {
                        delta.stats.crc_len_collision += 1;
                        continue;
                    }

                    let len = src_chunk.buf.len();
                    assert!(len > 0);
                    if src_chunk.buf != dest_chunk.buf {
                        delta.stats.crc_collision += 1;
                        continue;
                    }

                    match delta.matches.entry(dest_chunk.start) {
                        Entry::Vacant(e) => {
                            e.insert(Match {
                                buf: src_chunk.buf,
                                src_start: src_chunk.start,
                                dest_start: dest_chunk.start,
                            });
                            delta.stats.match_size += len as u64;
                        }
                        Entry::Occupied(_) => {}
                    }
                }
            }
        } else {
            delta.stats.crc_miss += 1;
        }
    }

    delta
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
            &Match {
                buf: single,
                src_start: 0,
                dest_start: 0,
            }
        );
        let delta = rollsum_delta(empty, single);
        assert_eq!(delta.matches.len(), 0);
    }
}
