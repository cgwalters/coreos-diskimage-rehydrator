use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use coreos_stream_metadata::Artifact;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use structopt::StructOpt;

#[deny(unused_must_use)]
mod rsync;

/// The target directory
const DIR: &str = "coreos-images-dehydrated";
const STREAM_FILE: &str = "stream.json";

// Number of CPUs we'll use
const N_WORKERS: u32 = 2;
const RSYNC_STRATEGY_DISK: &[&str] = &["openstack", "ibmcloud", "gcp"];

// TODO: aws.vmdk is internally compressed.  We need to replicate the qemu-img arguments,
// and also handle the CID.

// TODO: vmware.ova is a tarball - of metadata followed by the compressed disk image.
// To handle this fully reproducibly we'd need to try to regenerate the tarball
// bit-for bit which may be ugly.  But since we know nothing is *after* the disk image,
// it might work to literally save the tar headers and then generate the vmdk, then
// concatenate the two.

#[derive(Debug, StructOpt)]
struct DehydrateOpts {
    //    #[structopt(long)]
    //    artifact: Vec<String>,
    #[structopt(long)]
    skip_unavailable: bool,
}

#[derive(Debug, StructOpt)]
struct RehydrateOpts {
    /// Extract the disk image for a specific platform
    #[structopt(long)]
    disk: Vec<String>,

    /// Extract the metal ISO
    #[structopt(long)]
    iso: bool,

    /// Extract the metal PXE (kernel/initramfs/rootfs)
    #[structopt(long)]
    pxe: bool,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "coreos-diskimage-rehydrator")]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    /// Generate "dehydration files"
    Dehydrate(DehydrateOpts),
    /// Regenerate target file
    Rehydrate(RehydrateOpts),
}

fn run() -> Result<()> {
    match Opt::from_args() {
        Opt::Dehydrate(ref opts) => dehydrate(opts),
        Opt::Rehydrate(ref opts) => rehydrate(opts),
    }
}

fn rehydrate(opts: &RehydrateOpts) -> Result<(), anyhow::Error> {
    if opts.disk.len() == 0 && !opts.iso {
        return Err(anyhow!("No images specified"));
    }

    let srcdir = camino::Utf8Path::new(DIR);
    let stream_path = &srcdir.join(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: coreos_stream_metadata::Stream = serde_json::from_reader(std::io::BufReader::new(s))?;

    if opts.iso {}

    todo!()
}

fn uncompressed_name(s: &str) -> &str {
    s.strip_suffix(".xz")
        .or_else(|| s.strip_suffix(".gz"))
        .unwrap_or(s)
}

fn filename_for_artifact(a: &Artifact) -> Result<&str> {
    Ok(Utf8Path::new(&a.location)
        .file_name()
        .ok_or_else(|| anyhow!("Invalid artifact location: {}", a.location))?)
}

fn hardlink(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<()> {
    let src = src.as_ref();
    let dest = dest.as_ref();
    println!("Including: {:?}", src);
    std::fs::hard_link(src, dest).with_context(|| anyhow!("Hardlinking {:?}", src))?;
    Ok(())
}

/// Replace the input source file with a new zstd-compressed file ending in `.zst`.
fn zstd_compress(src: impl AsRef<Utf8Path>) -> Result<Utf8PathBuf> {
    let src = src.as_ref();
    let mut srcin = File::open(src)?;
    let dest = Utf8PathBuf::from(format!("{}.zst", src));
    let out = File::create(&dest)?;
    let mut out = zstd::Encoder::new(out, 10)?;
    std::io::copy(&mut srcin, &mut out)?;
    out.finish()?;
    std::fs::remove_file(src)?;
    Ok(dest)
}

fn dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let stream_path = Utf8Path::new(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: coreos_stream_metadata::Stream = serde_json::from_reader(std::io::BufReader::new(s))?;
    let thisarch = s
        .this_architecture()
        .ok_or_else(|| anyhow!("Missing this architecture in stream metadata"))?;
    let qemu = s
        .query_thisarch_single("qemu")
        .ok_or_else(|| anyhow!("Missing qemu image"))?;
    let qemu_fn = filename_for_artifact(qemu)?;
    let qemu_fn = Utf8Path::new(uncompressed_name(qemu_fn));
    if !qemu_fn.exists() {
        return Err(anyhow!("Missing uncompressed qemu image: {}", qemu_fn));
    }
    let destdir = camino::Utf8Path::new(DIR);
    std::fs::create_dir(destdir)
        .with_context(|| anyhow!("Failed to create destination directory: {}", destdir))?;

    hardlink(stream_path, destdir.join(stream_path.file_name().unwrap()))?;

    // Link in the qemu image now, we'll compress it at the end
    let qemu_dest = &destdir.join(qemu_fn);
    hardlink(qemu_fn, qemu_dest)?;

    let mut rsyncable: Vec<&Artifact> = RSYNC_STRATEGY_DISK
        .par_iter()
        .filter_map(|a| s.query_thisarch_single(a))
        .collect();

    if let Some(metal) = thisarch.artifacts.get("metal") {
        for (fmt, entries) in metal.formats.iter() {
            // The raw metal images are rsyncable
            if fmt.starts_with("raw.") || fmt.starts_with("4k.raw.") {
                rsyncable.push(
                    entries
                        .get("disk")
                        .ok_or_else(|| anyhow!("Missing disk entry for metal/{}", fmt))?,
                );
            } else {
                for a in entries.values() {
                    let name = Utf8Path::new(filename_for_artifact(a)?);
                    if opts.skip_unavailable && !name.exists() {
                        println!("Skipping: {}", name);
                    } else {
                        hardlink(name, destdir.join(name))?;
                    }
                }
            }
        }
    }

    // Add some parallelism
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| -> Result<_> {
        rsyncable.par_iter().try_for_each(|a| {
            let orig_fn = filename_for_artifact(a)?;
            let orig_fn = Utf8Path::new(uncompressed_name(orig_fn));
            if !orig_fn.exists() {
                if opts.skip_unavailable {
                    println!("Skipping: {}", orig_fn);
                    return Ok(());
                }
                return Err(anyhow!("Missing image: {}", orig_fn));
            }
            let delta_path = &destdir.join(format!("{}.rdelta", orig_fn));
            let output = std::io::BufWriter::new(File::create(delta_path)?);
            let mut output = zstd::Encoder::new(output, 10)?;
            rsync::prepare(qemu_fn, orig_fn, destdir, &mut output)?;
            output.flush()?;
            let orig_size = orig_fn.metadata()?.len() / (1000 * 1000);
            let delta_size = delta_path.metadata()?.len() / (1000 * 1000);
            println!(
                "Dehydrated: {} ({}%, {} MB)",
                orig_fn,
                ((delta_size as f64 / orig_size as f64) * 100f64).trunc() as u32,
                delta_size
            );
            Ok(())
        })
    })?;

    zstd_compress(qemu_dest)?;

    Ok(())
}

fn main() {
    // Print the error
    if let Err(e) = run() {
        eprintln!("{:#}", e);
        std::process::exit(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_suffix() {
        assert_eq!(uncompressed_name("foo.xz"), "foo");
    }
}
