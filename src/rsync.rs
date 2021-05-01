//use rayon::prelude::*;
use std::{collections::BTreeMap, convert::TryInto};

use crate::bupsplit;

const ROLLSUM_BLOB_MAX: usize = 8192 * 4;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Chunk {
    pub(crate) crc: u32,
    pub(crate) start: u64,
    pub(crate) len: u64,
}

pub(crate) fn rollsum_chunks_crc32(mut buf: &[u8]) -> BTreeMap<u32, Vec<Chunk>> {
    let mut ret = BTreeMap::<u32, Vec<Chunk>>::new();
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
        let mut crc = crc32fast::Hasher::new();
        crc.update(&buf[..ofs]);
        let crc = crc.finalize();

        let v = ret.entry(crc).or_default();
        v.push(Chunk {
            crc,
            start,
            len: ofs as u64,
        });
        start += ofs as u64;
        buf = &buf[ofs..]
    }
    ret
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RollsumDeltaStats {
    pub(crate) match_size: u64,
    pub(crate) crc_len_collision: u32,
    pub(crate) crc_collision: u32,
    pub(crate) src_chunks: u32,
    pub(crate) dest_chunks: u32,
}

#[derive(Debug, Default)]
pub(crate) struct RollsumDelta {
    pub(crate) matches: Vec<Match>,

    pub(crate) stats: RollsumDeltaStats,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct Match {
    pub(crate) crc: u32,
    pub(crate) len: u64,
    pub(crate) src_start: u64,
    pub(crate) dest_start: u64,
}

pub(crate) fn rollsum_delta(src: &[u8], dest: &[u8]) -> RollsumDelta {
    let mut delta: RollsumDelta = Default::default();
    let src_chunkset = rollsum_chunks_crc32(&src);
    let dest_chunkset = rollsum_chunks_crc32(&dest);

    delta.stats.src_chunks = src_chunkset.len().try_into().unwrap();
    delta.stats.dest_chunks = dest_chunkset.len().try_into().unwrap();

    for (&crc, dest_chunks) in dest_chunkset.iter() {
        if let Some(src_chunks) = src_chunkset.get(&crc) {
            for src_chunk in src_chunks.iter() {
                for dest_chunk in dest_chunks.iter() {
                    debug_assert_eq!(src_chunk.crc, dest_chunk.crc);

                    // Same crc32 but different length, skip it.
                    if src_chunk.len != dest_chunk.len {
                        delta.stats.crc_len_collision += 1;
                        continue;
                    }

                    let len = src_chunk.len;
                    assert!(len > 0);
                    let src_start = src_chunk.start;
                    let dest_start = dest_chunk.start;
                    let srcbuf = &src[src_start as usize..(src_start + len) as usize];
                    let destbuf = &dest[dest_start as usize..(dest_start + len) as usize];
                    if srcbuf != destbuf {
                        delta.stats.crc_collision += 1;
                        continue;
                    }

                    delta.matches.push(Match {
                        crc,
                        len,
                        src_start,
                        dest_start,
                    });
                    delta.stats.match_size += len;
                }
            }
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
            &delta.matches[0],
            &Match {
                crc: 163128923,
                len: 1,
                src_start: 0,
                dest_start: 0,
            }
        );
        let delta = rollsum_delta(empty, single);
        assert_eq!(delta.matches.len(), 0);
    }
}
