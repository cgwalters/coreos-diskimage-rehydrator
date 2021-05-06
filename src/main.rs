//! Main entrypoint to the rehydrator.

#![deny(unused_must_use)]
#![deny(unsafe_code)]

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use coreos_stream_metadata::Stream as CoreStream;
use coreos_stream_metadata::{Artifact, Stream};
use fn_error_context::context;
use rayon::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use structopt::StructOpt;

mod qemu_img;
mod rsync;
mod utils;

/// The target directory
const DIR: &str = "coreos-images-dehydrated";
/// Where we put temporarily decompressed images
const CACHEDIR: &str = "dehydrate-cache";
/// The name of our stream file
const STREAM_FILE: &str = "stream.json";
/// Number of CPUs we'll use
const N_WORKERS: u32 = 2;
/// The qemu name
const QEMU: &str = "qemu";
/// AWS is vmdk so handled specially
const AWS: &str = "aws";
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

/// Commands used to dehydrate images
#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Build {
    /// Initialize from a stream
    Init {
        /// Stream ID (e.g. `stable` for FCOS, `rhcos-4.8` for RHCOS)
        stream: String,
    },
    /// Download all supported images
    Download,
    /// Generate "dehydration files"
    Dehydrate(DehydrateOpts),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "coreos-diskimage-rehydrator")]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    PrintStreamJson,
    Build(Build),
    /// Regenerate target file
    Rehydrate(RehydrateOpts),
}

fn run() -> Result<()> {
    match Opt::from_args() {
        Opt::PrintStreamJson => {
            let srcdir = camino::Utf8Path::new(DIR);
            let mut f = BufReader::new(File::open(srcdir.join("stream.json"))?);
            let out = std::io::stdout();
            let mut out = out.lock();
            std::io::copy(&mut f, &mut out)?;
            Ok(())
        }
        Opt::Build(b) => match b {
            Build::Init { ref stream } => build_init(stream.as_str()),
            Build::Download => build_download(),
            Build::Dehydrate(ref opts) => build_dehydrate(opts),
        },
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
        .unwrap_or_else(|| a.sha256.as_str());
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
    if opts.disk.is_empty() && !opts.iso {
        return Err(anyhow!("No images specified"));
    }

    let srcdir = camino::Utf8Path::new(DIR);
    let stream_path = &srcdir.join(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: CoreStream = serde_json::from_reader(std::io::BufReader::new(s))?;
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

    if !opts.disk.is_empty() {
        // Need to decompress the qemu image
        let qemu = s
            .query_thisarch_single(QEMU)
            .ok_or_else(|| anyhow!("Missing {}", QEMU))?;
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
            .filter(|s| s.as_str() != QEMU)
            .try_for_each(|disk| {
                let a = s
                    .query_thisarch_single(disk)
                    .ok_or_else(|| anyhow!("Failed to find disk for {}", disk))?;
                let artifact_filename = Utf8Path::new(filename_for_artifact(a)?);
                let uncompressed_name =
                    Utf8Path::new(uncompressed_name(artifact_filename.as_str()));
                let patch = srcdir.join(rdelta_name_for_artifact(a)?);
                let tmpname = &Utf8PathBuf::from(format!("{}.tmp", uncompressed_name));
                rsync::apply(qemu_fn, tmpname.as_str(), Utf8Path::new("."), patch)?;
                if uncompressed_name.extension() == Some(qemu_img::VMDK) {
                    println!("Regenerating VMDK for: {}", disk); // 😢
                    qemu_img::copy_to_vmdk(tmpname, uncompressed_name)?;
                    std::fs::remove_file(tmpname)?;
                    println!(
                        "Generated (but skipped SHA-256 validation due to vmdk compression): {}",
                        uncompressed_name
                    );
                } else {
                    std::fs::rename(tmpname, uncompressed_name)?;
                    validate(opts, a, uncompressed_name)?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        if opts.disk.iter().any(|s| s.as_str() == QEMU) {
            print!("Generated: {}", qemu_fn);
        }
    }

    Ok(())
}

fn maybe_uncompressed_name(s: &str) -> Option<&str> {
    s.strip_suffix(".xz").or_else(|| s.strip_suffix(".gz"))
}

fn uncompressed_name(s: &str) -> &str {
    maybe_uncompressed_name(s).unwrap_or(s)
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

#[context("Creating rsync delta")]
fn rsync_delta(
    _opts: &DehydrateOpts,
    src: &Artifact,
    target: &Artifact,
    destdir: impl AsRef<Utf8Path>,
) -> Result<bool> {
    let destdir = destdir.as_ref();

    let src_fn = &get_maybe_uncompressed(src)?;
    let target_fn = &get_maybe_uncompressed(target)?;

    let delta_path = &destdir.join(rdelta_name_for_artifact(target)?);
    let output = std::io::BufWriter::new(File::create(delta_path)?);
    // zstd encode the rsync delta because it saves space.
    let mut output = zstd::Encoder::new(output, 7)?;
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

fn read_stream() -> Result<CoreStream> {
    let stream_path = Utf8Path::new(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: CoreStream = serde_json::from_reader(std::io::BufReader::new(s))?;
    Ok(s)
}

fn cached_uncompressed_name(a: &Artifact) -> Result<Option<(Utf8PathBuf, bool)>> {
    let name = filename_for_artifact(a)?;
    let r = maybe_uncompressed_name(name)
        .map(|uncomp_name| Utf8Path::new(CACHEDIR).join(uncomp_name))
        .map(|p| {
            if p.extension() == Some(qemu_img::VMDK) {
                (p.with_extension(qemu_img::QCOW2), true)
            } else {
                (p, false)
            }
        });
    Ok(r)
}

fn uncompressor_for(name: &Utf8Path, src: impl Read) -> Result<impl Read> {
    let r = match name.extension() {
        Some("xz") => either::Left(xz2::read::XzDecoder::new(src)),
        Some("gz") => either::Right(flate2::read::GzDecoder::new(src)),
        Some(other) => return Err(anyhow!("Unknown extension {}", other)),
        None => return Err(anyhow!("No extension found for {}", name)),
    };
    Ok(r)
}

fn get_maybe_uncompressed(a: &Artifact) -> Result<Utf8PathBuf> {
    let name = Utf8Path::new(filename_for_artifact(a)?);
    let uncomp_name = cached_uncompressed_name(a)?;
    let r = uncomp_name
        .map(|(uncomp_name, is_vmdk)| {
            if !uncomp_name.exists() {
                let src = File::open(name).with_context(|| anyhow!("Failed to open {}", name))?;
                let tmpname = format!("{}.tmp", uncomp_name);
                let mut src = uncompressor_for(name, src)?;
                let mut dest = std::io::BufWriter::new(File::create(&tmpname)?);
                std::io::copy(&mut src, &mut dest)?;
                dest.flush()?;
                if is_vmdk {
                    qemu_img::copy_to_qcow2(&tmpname, &uncomp_name)?;
                    std::fs::remove_file(tmpname)?;
                    println!("Converted to uncompressed qcow2: {}", name);
                } else {
                    std::fs::rename(&tmpname, &uncomp_name)?;
                }
                println!("Uncompressed: {}", uncomp_name);
            }
            Ok::<_, anyhow::Error>(uncomp_name)
        })
        .transpose()?;
    Ok(r.unwrap_or_else(|| name.into()))
}

// Generate an image from its rsync delta.
fn dehydrate_rsyncable(
    opts: &DehydrateOpts,
    s: &Stream,
    qemu: &Artifact,
    name: &str,
    destdir: &Utf8Path,
) -> Result<()> {
    let a = s
        .query_thisarch_single(name)
        .ok_or_else(|| anyhow!("Missing artifact {}", name))?;
    let _found: bool = rsync_delta(opts, qemu, a, destdir)?;
    Ok(())
}

/// Loop over stream metadata and generate dehydrated (~deduplicated) content.
fn build_dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let stream_path = Utf8Path::new(STREAM_FILE);
    let s = read_stream()?;

    std::fs::create_dir_all(CACHEDIR).context("Creating cachedir")?;

    let thisarch = s
        .this_architecture()
        .ok_or_else(|| anyhow!("Missing this architecture in stream metadata"))?;
    let qemu = s
        .query_thisarch_single("qemu")
        .ok_or_else(|| anyhow!("Missing qemu image"))?;
    let uncomp_qemu = &get_maybe_uncompressed(qemu)?;
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
    let qemu_dest = &destdir.join(uncomp_qemu.file_name().unwrap());
    hardlink(uncomp_qemu, qemu_dest)?;

    // Add some parallelism
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| {
        // The rsyncable artifacts are easy.
        RSYNC_STRATEGY_DISK
            .par_iter()
            .map(|&name| dehydrate_rsyncable(opts, &s, qemu, name, destdir))
            .chain(
                rayon::iter::once(AWS)
                    .map(|name| dehydrate_rsyncable(opts, &s, qemu, name, destdir)),
            )
            .try_reduce(|| (), |_, _| Ok(()))?;
        Ok::<_, anyhow::Error>(())
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

fn build_init(stream: &str) -> Result<()> {
    let u = coreos_stream_metadata::stream_url_from_id(stream)?;
    if Utf8Path::new(STREAM_FILE).exists() {
        return Err(anyhow!("{} exists, not overwriting", STREAM_FILE));
    }
    println!("Downloading {}", u);
    let mut out = std::io::BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(STREAM_FILE)?,
    );
    let mut resp = reqwest::blocking::get(&u)?;
    resp.copy_to(&mut out)?;
    out.flush()?;
    Ok(())
}

fn build_download() -> Result<()> {
    let s = read_stream()?;
    let thisarch = s
        .this_architecture()
        .ok_or_else(|| anyhow!("Missing this architecture in stream metadata"))?;
    let client = reqwest::blocking::ClientBuilder::new()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION"),
        ))
        .https_only(true)
        .build()?;
    let artifacts = {
        let mut artifacts = Vec::new();
        for name in RSYNC_STRATEGY_DISK.iter().chain(std::iter::once(&QEMU)) {
            let a = s
                .query_thisarch_single(name)
                .ok_or_else(|| anyhow!("Missing {}", name))?;
            artifacts.push(a);
        }
        if let Some(metal) = thisarch.artifacts.get("metal") {
            if let Some(pxe) = metal.formats.get("pxe") {
                let rootfs = pxe
                    .get("rootfs")
                    .ok_or_else(|| anyhow!("Missing metal/pxe/rootfs"))?;
                artifacts.push(rootfs)
            }
            // If we have an ISO, delta it from the rootfs
            if let Some(iso) = metal.formats.get("iso") {
                artifacts.push(iso.get("disk").ok_or_else(|| anyhow!("Invalid iso"))?);
            }
        }
        artifacts
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| -> Result<_> {
        artifacts.par_iter().try_for_each_init(
            || client.clone(),
            |client, &a| -> Result<()> {
                let fname = Utf8Path::new(filename_for_artifact(a)?);
                if fname.exists() {
                    return Ok(());
                }
                let temp_name = &format!("{}.tmp", fname);
                let mut out = std::io::BufWriter::new(File::create(temp_name)?);
                let mut resp = client.get(a.location.as_str()).send()?;
                resp.copy_to(&mut out)
                    .with_context(|| anyhow!("Failed to download {}", a.location))?;
                std::fs::rename(temp_name, fname)?;
                println!("Downloaded: {}", fname);
                Ok(())
            },
        )
    })?;
    Ok(())
}
