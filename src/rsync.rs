use std::collections::BTreeMap;

use crate::bupsplit;

const ROLLSUM_BLOB_MAX: usize = 8192 * 4;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Chunk {
    crc: u32,
    start: u64,
    ofs: u64,
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
            ofs: ofs as u64,
        });
        start += ofs as u64;
        buf = &buf[ofs..]
    }
    ret
}
