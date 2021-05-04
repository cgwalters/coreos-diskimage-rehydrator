//! Main entrypoint to the rehydrator.

#![deny(unused_must_use)]
#![deny(unsafe_code)]

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use coreos_stream_metadata::Artifact;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use structopt::StructOpt;

mod rsync;
mod utils;

/// The target directory
const DIR: &str = "coreos-images-dehydrated";
const STREAM_FILE: &str = "stream.json";

// Number of CPUs we'll use
const N_WORKERS: u32 = 2;
// openstack and ibmcloud are just qcow2 images.
// gcp is a tarball with a sparse disk image inside it, but for rsync that's
// not really different than a qcow2.
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

    /// Don't recompress images
    #[structopt(long)]
    skip_compress: bool,

    /// Don't verify SHA-256 of generated images
    #[structopt(long)]
    skip_validate: bool,
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

fn validate(opts: &RehydrateOpts, a: &Artifact, target: impl AsRef<Utf8Path>) -> Result<()> {
    let target = target.as_ref();
    if opts.skip_validate {
        println!("Generated (but skipped SHA-256 validation): {}", target);
        return Ok(());
    }
    let expected = a
        .uncompressed_sha256
        .as_deref()
        .unwrap_or(a.sha256.as_str());
    let actual = utils::sha256_file(target)?;
    if expected != actual {
        return Err(anyhow!(
            "SHA-256 mismatch for {} - expected: {} actual: {}",
            target,
            expected,
            actual
        ));
    }
    println!("Generated: {}", target);
    Ok(())
}

fn rehydrate(opts: &RehydrateOpts) -> Result<(), anyhow::Error> {
    if opts.disk.len() == 0 && !opts.iso {
        return Err(anyhow!("No images specified"));
    }

    let srcdir = camino::Utf8Path::new(DIR);
    let stream_path = &srcdir.join(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: coreos_stream_metadata::Stream = serde_json::from_reader(std::io::BufReader::new(s))?;
    let thisarch = s
        .this_architecture()
        .ok_or_else(|| anyhow!("Missing this architecture in stream metadata"))?;
    if opts.iso {
        let metal = thisarch
            .artifacts
            .get("metal")
            .ok_or_else(|| anyhow!("Missing metal"))?;
        let rootfs = metal
            .formats
            .get("pxe")
            .map(|p| p.get("rootfs"))
            .flatten()
            .ok_or_else(|| anyhow!("Missing metal/pxe/rootfs"))?;
        let iso = metal
            .formats
            .get("iso")
            .map(|p| p.get("disk"))
            .flatten()
            .ok_or_else(|| anyhow!("Missing metal/iso/disk"))?;
        let iso_fn = filename_for_artifact(iso)?;
        let patch = srcdir.join(rdelta_name_for_artifact(iso)?);
        rsync::apply(
            &srcdir.join(filename_for_artifact(rootfs)?),
            iso_fn,
            Utf8Path::new("."),
            patch,
        )?;
        validate(opts, iso, iso_fn)?;
    }

    if opts.pxe {
        todo!()
    }

    if opts.disk.len() > 0 {
        // Need to decompress the qemu image
        let qemu = s
            .query_thisarch_single("qemu")
            .ok_or_else(|| anyhow!("Missing qemu"))?;
        let qemu_fn = Utf8Path::new(uncompressed_name(filename_for_artifact(qemu)?));
        if !qemu_fn.exists() {
            {
                let qemu_zstd_path =
                    srcdir.join(format!("{}.zst", uncompressed_name(qemu_fn.as_str())));
                println!("Decompressing: {}", qemu_zstd_path);
                let f = File::open(&qemu_zstd_path)
                    .with_context(|| anyhow!("Opening {}", qemu_zstd_path))?;
                let mut f = zstd::Decoder::new(f)?;
                let mut o = std::io::BufWriter::new(
                    File::create(qemu_fn).context("Opening qemu destination")?,
                );
                std::io::copy(&mut f, &mut o).context("Failed to decompress qemu")?;
                o.flush()?;
            }
            validate(opts, qemu, qemu_fn)?;
        }
        opts.disk
            .par_iter()
            .filter(|s| s.as_str() != "qemu")
            .try_for_each(|disk| {
                let a = s
                    .query_thisarch_single(disk)
                    .ok_or_else(|| anyhow!("Failed to find disk for {}", disk))?;
                let patch = srcdir.join(rdelta_name_for_artifact(a)?);
                let target_fn = uncompressed_name(filename_for_artifact(a)?);
                rsync::apply(qemu_fn, target_fn, Utf8Path::new("."), patch)?;
                validate(opts, a, target_fn)?;
                Ok::<_, anyhow::Error>(())
            })?;
        if opts.disk.iter().find(|s| s.as_str() == "qemu").is_some() {
            print!("Generated: {}", qemu_fn);
        }
    }

    Ok(())
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

fn rdelta_name_for_artifact(a: &Artifact) -> Result<String> {
    Ok(format!(
        "{}.rdelta",
        uncompressed_name(filename_for_artifact(a)?)
    ))
}

fn rsync_delta(
    opts: &DehydrateOpts,
    src: &Artifact,
    target: &Artifact,
    destdir: impl AsRef<Utf8Path>,
) -> Result<bool> {
    let destdir = destdir.as_ref();
    let target_fn = Utf8Path::new(uncompressed_name(filename_for_artifact(target)?));
    if !target_fn.exists() {
        if opts.skip_unavailable {
            println!("Skipping: {}", target_fn);
            return Ok(false);
        }
        return Err(anyhow!("Missing image: {}", target_fn));
    }
    let src_fn = Utf8Path::new(uncompressed_name(filename_for_artifact(src)?));
    let delta_path = &destdir.join(rdelta_name_for_artifact(target)?);
    let output = std::io::BufWriter::new(File::create(delta_path)?);
    // zstd encode the rsync delta because it saves space.
    let mut output = zstd::Encoder::new(output, 10)?;
    rsync::prepare(src_fn, target_fn, destdir, &mut output)?;
    output.finish()?;
    let orig_size = target_fn.metadata()?.len();
    let delta_size = delta_path.metadata()?.len();
    println!(
        "Dehydrated: {} ({:.5}%, {})",
        target_fn,
        ((delta_size as f64 / orig_size as f64) * 100f64),
        indicatif::HumanBytes(delta_size)
    );
    Ok(true)
}

/// Loop over stream metadata and generate dehydrated (~deduplicated) content.
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

    if let Some(metal) = thisarch.artifacts.get("metal") {
        // The rootfs (squashfs-in-cpio) is a source artifact for the ISO
        let rootfs = if let Some(pxe) = metal.formats.get("pxe") {
            let rootfs = pxe
                .get("rootfs")
                .ok_or_else(|| anyhow!("Missing metal/pxe/rootfs"))?;
            let rootfs_name = filename_for_artifact(rootfs)?;
            hardlink(rootfs_name, destdir.join(rootfs_name))?;
            Some(rootfs)
        } else {
            None
        };
        // If we have an ISO, delta it from the rootfs
        if let Some(iso) = metal.formats.get("iso") {
            let iso = iso
                .get("disk")
                .ok_or_else(|| anyhow!("Missing disk for metal/iso"))?;
            let rootfs = rootfs.ok_or_else(|| anyhow!("Found iso without pxe/rootfs"))?;
            let _found: bool = rsync_delta(opts, rootfs, iso, destdir)?;
        }
    }

    // Link in the qemu image now, we'll compress it at the end
    let qemu_dest = &destdir.join(qemu_fn);
    hardlink(qemu_fn, qemu_dest)?;

    let qemu_rsyncable: Vec<&Artifact> = RSYNC_STRATEGY_DISK
        .par_iter()
        .filter_map(|a| s.query_thisarch_single(a))
        .collect();

    // Add some parallelism
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| -> Result<_> {
        qemu_rsyncable.par_iter().try_for_each(|a| {
            let _found: bool = rsync_delta(opts, qemu, a, destdir)?;
            Ok(())
        })
    })?;

    println!("Including (zstd compressed): {}", qemu_dest);
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
