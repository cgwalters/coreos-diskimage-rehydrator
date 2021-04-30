use std::{fs::File, path::Path};

use anyhow::Result;
use memmap2::{Mmap, MmapOptions, MmapRaw};
use structopt::StructOpt;
use rayon::prelude::*;

mod bupsplit;
mod rsync;

#[derive(Debug, StructOpt)]
struct DehydrateOpts {
    /// Path to qemu image
    src_qemu: String,

    dest_img: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "coreos-diskimage-rehydrator")]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    /// Generate "dehydration files"
    Dehydrate(DehydrateOpts),
}

fn run() -> Result<()> {
    match Opt::from_args() {
        Opt::Dehydrate(ref opts) => dehydrate(opts),
    }
}

fn mmap<P: AsRef<Path>>(p: P) -> Result<Mmap> {
    let p = p.as_ref();
    let f = File::open(p)?;
    Ok(unsafe { MmapOptions::new().map(&f)? })
}

struct Match {
    crc: u32,
}

fn dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let src = mmap(opts.src_qemu.as_str())?;
    let dest = mmap(opts.dest_img.as_str())?;
    let src_chunkset = rsync::rollsum_chunks_crc32(&src);
    let dest_chunkset = rsync::rollsum_chunks_crc32(&dest);

    let mut matches = Vec::new();

    for (&crc, dest_chunks) in dest_chunkset.iter() {
        if let Some(src_chunks) = src_chunkset.get(&crc) {
            for src_chunk in src_chunks.iter() {
                for dest_chunk in dest_chunks.iter() {
                    debug_assert_eq!(src_chunk.crc, dest_chunk.crc);

                    // Same crc32 but different length, skip it.
                    if src_chunk.ofs != dest_chunk.ofs {
                        continue;
                    }

                    let srcbuf: &[u8] =
                        &src[src_chunk.start as usize..(src_chunk.start + src_chunk.ofs) as usize];
                    let destbuf = &dest
                        [dest_chunk.start as usize..(dest_chunk.start + dest_chunk.ofs) as usize];
                    if srcbuf != destbuf {
                        continue;
                    }

                    matches.push(Match { crc })
                }
            }
        }
    }
    println!("Source chunks: {}", src_chunkset.len());
    println!("Dest chunks: {}", dest_chunkset.len());
    println!("Matches: {}", matches.len());

    Ok(())
}

fn main() {
    // Print the error
    if let Err(e) = run() {
        eprintln!("{:#}", e);
        std::process::exit(1)
    }
}
