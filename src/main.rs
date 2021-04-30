use std::fs::File;

use anyhow::Result;
use memmap2::MmapOptions;
use structopt::StructOpt;

mod bupsplit;
mod rsync;

#[derive(Debug, StructOpt)]
struct DehydrateOpts {
    /// Path to qemu image
    src_qemu: String,
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

fn dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let file = File::open(opts.src_qemu.as_str())?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let chunks = rsync::rollsum_chunks_crc32(&mmap);
    println!("Chunks: {}", chunks.len());
    Ok(())
}

fn main() {
    // Print the error
    if let Err(e) = run() {
        eprintln!("{:#}", e);
        std::process::exit(1)
    }
}
