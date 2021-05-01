use std::{fs::File, path::Path};

use anyhow::Result;
use memmap2::{Mmap, MmapOptions};
use structopt::StructOpt;

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

fn dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let src = mmap(opts.src_qemu.as_str())?;
    let dest = mmap(opts.dest_img.as_str())?;

    let delta = rsync::rollsum_delta(&src, &dest);

    println!("{:?}", delta.stats);

    Ok(())
}

fn main() {
    // Print the error
    if let Err(e) = run() {
        eprintln!("{:#}", e);
        std::process::exit(1)
    }
}
