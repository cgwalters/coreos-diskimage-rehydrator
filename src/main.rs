//! Main entrypoint to the rehydrator.

#![deny(unused_must_use)]
#![deny(unsafe_code)]

use crate::riverdelta::{ArtifactExt, RiverDelta};
use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use coreos_stream_metadata::Artifact;
use coreos_stream_metadata::Stream as CoreStream;
use fn_error_context::context;
use rayon::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;
use tracing::{debug, info};

mod download;
mod ova;
mod qemu_img;
mod riverdelta;
mod rsync;
mod utils;

/// The target directory
const DIR: &str = "coreos-images-dehydrated";
/// Where we put temporarily decompressed images
const CACHEDIR: &str = "dehydrate-cache";
/// The name of our stream file
const STREAM_FILE: &str = "stream.json";
/// Name of metadata file
const METADATA_FILE: &str = "meta.json";
/// Number of CPUs we'll use
pub(crate) const N_WORKERS: u32 = 3;

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

    /// Don't verify SHA-256 of generated images
    #[structopt(long)]
    skip_validate: bool,

    /// Directory to use for image output.  If `-`, use stdout.
    /// If multiple images are specified with `-`, then a GNU tar
    /// stream will be used that can be uncompressed by piping
    /// to e.g. `| tar xf -`.
    dest: String,
}

#[derive(Debug, StructOpt, Default)]
struct DehydrateOpts {
    /// Do not fatally error if there are unhandled artifacts.
    #[structopt(long)]
    allow_unhandled: bool,
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
    /// Generate "dehydration files" from already downloaded files
    Dehydrate(DehydrateOpts),
    /// Remove cached files
    Clean,
    /// Initialize, download, and dehydrate in one go
    Run {
        /// Stream ID (e.g. `stable` for FCOS, `rhcos-4.8` for RHCOS)
        stream: String,
    },
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

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    original_artifact_size: u64,
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
            Build::Download => download::build_download(),
            Build::Dehydrate(ref opts) => build_dehydrate(opts),
            Build::Clean => build_clean(),
            Build::Run { ref stream } => {
                build_init(stream.as_str())?;
                download::build_download()?;
                build_dehydrate(&Default::default())?;
                build_clean()?;
                Ok(())
            }
        },
        Opt::Rehydrate(ref opts) => rehydrate(opts),
    }
}

/// Initialize directory with stream data
fn build_init(stream: &str) -> Result<()> {
    let u = coreos_stream_metadata::stream_url_from_id(stream)?;
    if Utf8Path::new(STREAM_FILE).exists() {
        return Err(anyhow!("{} exists, not overwriting", STREAM_FILE));
    }
    info!("Downloading {}", u);
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

/// Clean cached data from the current directory.
fn build_clean() -> Result<()> {
    let cachedir = Utf8Path::new(CACHEDIR);
    if cachedir.exists() {
        std::fs::remove_dir_all(cachedir)?;
        info!("Removed: {}", CACHEDIR);
    }
    Ok(())
}

enum OutputTarget<W: std::io::Write> {
    Directory(Utf8PathBuf),
    Stdout(W),
    Tar(tar::Builder<W>),
}

struct RehydrateContext<'a, 'b, W: std::io::Write> {
    opts: &'a RehydrateOpts,

    target: Arc<Mutex<OutputTarget<W>>>,
    tmpdir: &'b Utf8Path,
}

fn write_output<W: std::io::Write>(
    ctx: &RehydrateContext<W>,
    target: impl AsRef<Utf8Path>,
) -> Result<()> {
    let target = target.as_ref();
    let mut outtarget = ctx.target.lock().unwrap();
    match &mut *outtarget {
        OutputTarget::Directory(ref d) => {
            std::fs::rename(target, d.join(target.file_name().unwrap()))
                .with_context(|| format!("Failed to move {} to {}", target, d))?;
        }
        OutputTarget::Stdout(ref mut s) => {
            let mut src = std::io::BufReader::new(File::open(target)?);
            std::io::copy(&mut src, s)?;
        }
        OutputTarget::Tar(ref mut t) => {
            let mut src = File::open(target)?;
            t.append_file(target.file_name().unwrap(), &mut src)?;
        }
    }
    Ok(())
}

/// Generate a new temporary hardlink.
///
/// We do this gyration because most of our code ends up generating
/// new files that we want to `rename()`.
fn temp_hardlink(src: impl AsRef<Utf8Path>, tmpdir: impl AsRef<Utf8Path>) -> Result<Utf8PathBuf> {
    let src = src.as_ref();
    let tmpdir = tmpdir.as_ref();
    let dest = tmpdir.join(src.file_name().unwrap());
    std::fs::hard_link(src, &dest).with_context(|| anyhow!("Failed to hardlink {}", src))?;
    Ok(dest)
}

fn finish_output<W: std::io::Write>(
    ctx: &RehydrateContext<W>,
    a: &Artifact,
    target: impl AsRef<Utf8Path>,
) -> Result<()> {
    let target = target.as_ref();
    if ctx.opts.skip_validate {
        info!("Generated (but skipped SHA-256 validation): {}", target);
        return write_output(ctx, target);
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
    debug!("Validated {}", expected);
    info!("Generated: {}", target);
    write_output(ctx, target)
}

fn rehydrate(opts: &RehydrateOpts) -> Result<(), anyhow::Error> {
    let pxe_or_iso = opts.iso || opts.pxe;
    if opts.disk.is_empty() && !pxe_or_iso {
        return Err(anyhow!("No images specified"));
    }

    let tmpdir = tempfile::tempdir_in(".")?;
    let tmpdir: &Utf8Path = tmpdir.path().try_into()?;

    // PXE is multiple things.
    let have_multiple = opts.disk.len() > 1 || opts.pxe;
    let stdout = std::io::stdout();
    let is_stdout = opts.dest == "-";
    if is_stdout && nix::unistd::isatty(1)? {
        return Err(anyhow!("Refusing to output to a tty"));
    }
    let target = match (is_stdout, have_multiple) {
        (true, true) => OutputTarget::Tar(tar::Builder::new(stdout)),
        (true, false) => OutputTarget::Stdout(stdout),
        (_, _) => OutputTarget::Directory(opts.dest.clone().into()),
    };

    let ctx = &RehydrateContext {
        opts,
        tmpdir,
        target: Arc::new(Mutex::new(target)),
    };

    let srcdir = camino::Utf8Path::new(DIR);
    let stream_path = &srcdir.join(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: CoreStream = serde_json::from_reader(std::io::BufReader::new(s))?;
    let riverdelta: RiverDelta = s.try_into()?;
    if opts.iso {
        let metal = riverdelta
            .metal
            .as_ref()
            .ok_or_else(|| anyhow!("Missing metal"))?;
        let iso_fn = metal.iso.filename();
        let patch = srcdir.join(rdelta_name_for_artifact(&metal.iso)?);
        rsync::apply(
            &srcdir.join(metal.pxe.rootfs.filename()),
            iso_fn,
            Utf8Path::new("."),
            patch,
        )?;
        finish_output(ctx, &metal.iso, iso_fn)?;
    }
    if opts.pxe {
        let metal = riverdelta
            .metal
            .as_ref()
            .ok_or_else(|| anyhow!("Missing metal"))?;
        for a in [&metal.pxe.kernel, &metal.pxe.initramfs, &metal.pxe.rootfs].iter() {
            let src = srcdir.join(a.filename());
            let tmp = temp_hardlink(src, tmpdir)?;
            finish_output(ctx, a, &tmp)?;
        }
    }

    let qemu = &riverdelta.qemu;
    let qemu_fn = Utf8Path::new(uncompressed_name(qemu.filename()));
    if !opts.disk.is_empty() {
        // Need to decompress the qemu image
        if !qemu_fn.exists() {
            {
                let qemu_zstd_path =
                    srcdir.join(format!("{}.zst", uncompressed_name(qemu_fn.as_str())));
                info!("Decompressing: {}", qemu_zstd_path);
                let f = File::open(&qemu_zstd_path)
                    .with_context(|| anyhow!("Opening {}", qemu_zstd_path))?;
                let mut f = zstd::Decoder::new(f)?;
                let mut o = std::io::BufWriter::new(
                    File::create(qemu_fn).context("Opening qemu destination")?,
                );
                std::io::copy(&mut f, &mut o).context("Failed to decompress qemu")?;
                o.flush()?;
            }
            info!("Unpacked source image: {}", qemu_fn);
        }
    }
    // Now build a hash set so we can conveniently look up bits, filter out qemu
    // since we're done with that.
    let mut disks: HashSet<_> = opts
        .disk
        .iter()
        .map(|s| s.as_str())
        .filter(|&s| s != riverdelta::QEMU)
        .collect();

    // Handle non-rsyncable targets.
    if disks.take("vmware").is_some() {
        let vmware = riverdelta
            .vmware
            .as_ref()
            .ok_or_else(|| anyhow!("No vmware artifact available"))?;
        rehydrate_ova(ctx, qemu_fn, vmware)?;
    }

    // And the remainder must be rsyncable, or we fail.
    disks.par_iter().try_for_each(|&disk| {
        if riverdelta.unhandled.contains_key(disk) {
            return Err(anyhow!("Unhandled artifact: {}", disk));
        }
        let a = riverdelta
            .get_rsyncable(disk)
            .ok_or_else(|| anyhow!("Unknown artifact: {}", disk))?;
        let artifact_filename = Utf8Path::new(a.filename());
        let uncompressed_name = Utf8Path::new(uncompressed_name(artifact_filename.as_str()));
        let patch = srcdir.join(rdelta_name_for_artifact(a)?);
        let tmpname = &Utf8PathBuf::from(format!("{}.tmp", uncompressed_name));
        rsync::apply(qemu_fn, tmpname.as_str(), Utf8Path::new("."), patch)?;
        if uncompressed_name.extension() == Some(qemu_img::VMDK) {
            info!("Regenerating VMDK for: {}", disk); // 😢
            qemu_img::copy_to_vmdk(tmpname, uncompressed_name)?;
            std::fs::remove_file(tmpname)?;
            info!(
                "Generated (but skipped SHA-256 validation due to vmdk compression): {}",
                uncompressed_name
            );
        } else {
            std::fs::rename(tmpname, uncompressed_name)?;
            finish_output(ctx, a, uncompressed_name)?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    if opts.disk.iter().any(|s| s.as_str() == riverdelta::QEMU) {
        print!("Generated: {}", qemu_fn);
    }

    let mut target = ctx.target.lock().unwrap();
    match &mut *target {
        OutputTarget::Directory(_) => {}
        OutputTarget::Stdout(s) => {
            s.flush()?;
        }
        OutputTarget::Tar(t) => {
            t.finish()?;
        }
    }

    Ok(())
}

fn temppath_name(t: &tempfile::TempPath) -> Result<&Utf8Path> {
    let p: &Path = t.as_ref();
    let r = p.try_into()?;
    Ok(r)
}

fn tempfile_name(t: &tempfile::NamedTempFile) -> Result<&Utf8Path> {
    let p = t.path();
    let r = p.try_into()?;
    Ok(r)
}

fn rehydrate_ova<W: std::io::Write>(
    ctx: &RehydrateContext<W>,
    qemu_path: impl AsRef<Utf8Path>,
    vmware: &Artifact,
) -> Result<()> {
    let srcdir = camino::Utf8Path::new(DIR);
    let qemu_path = qemu_path.as_ref();
    let delta_ova_name = &srcdir.join(ova_rdelta_name_for_artifact(vmware));
    let target_ova_name = vmware.filename();
    let mut temp_delta = tempfile::NamedTempFile::new_in(ctx.tmpdir)?;
    let ova_meta = ova::ova_extract(delta_ova_name, &mut temp_delta)?;
    temp_delta.flush()?;
    let temp_delta = temp_delta.into_temp_path();
    let temp_delta: &Path = temp_delta.as_ref();
    let temp_delta: &Utf8Path = temp_delta.try_into()?;
    let temp_qcow2 = tempfile::NamedTempFile::new_in(ctx.tmpdir)?;
    rsync::apply(
        qemu_path,
        tempfile_name(&temp_qcow2)?.as_str(),
        ctx.tmpdir,
        temp_delta,
    )?;
    drop(temp_delta);
    let temp_vmdk = tempfile::NamedTempFile::new_in(ctx.tmpdir)?.into_temp_path();
    info!("Regenerating VMDK for: {}", target_ova_name); // 😢
    qemu_img::copy_to_vmdk(tempfile_name(&temp_qcow2)?, temppath_name(&temp_vmdk)?)?;
    drop(temp_qcow2);
    let temp_ova = &ctx.tmpdir.join(target_ova_name);
    let mut temp_ova_f = BufWriter::new(File::create(temp_ova)?);
    ova::ova_rebuild(&ova_meta, temppath_name(&temp_vmdk)?, &mut temp_ova_f)?;
    temp_ova_f.flush()?;
    info!(
        "Generated (but skipped SHA-256 validation due to vmdk compression): {}",
        target_ova_name
    );
    write_output(ctx, temp_ova)?;
    Ok(())
}

fn maybe_uncompressed_name(s: &str) -> Option<&str> {
    s.strip_suffix(".xz").or_else(|| s.strip_suffix(".gz"))
}

fn uncompressed_name(s: &str) -> &str {
    maybe_uncompressed_name(s).unwrap_or(s)
}

fn hardlink(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<()> {
    let src = src.as_ref();
    let dest = dest.as_ref();
    info!("Including: {:?}", src);
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
    Ok(format!("{}.rdelta", uncompressed_name(a.filename())))
}

fn ova_rdelta_name_for_artifact(a: &Artifact) -> String {
    format!("{}.ova-rdelta", uncompressed_name(a.filename()))
}

fn rsync_delta_impl(
    src_fn: impl AsRef<Utf8Path>,
    target: impl AsRef<Utf8Path>,
    delta_path: impl AsRef<Utf8Path>,
) -> Result<()> {
    let src_fn = src_fn.as_ref();
    let target_fn = target.as_ref();
    let delta_path = delta_path.as_ref();
    let mut output = std::io::BufWriter::new(File::create(delta_path)?);
    rsync::prepare(src_fn, target_fn, delta_path.parent().unwrap(), &mut output)?;
    output.flush()?;
    let orig_size = target_fn.metadata()?.len();
    let delta_size = delta_path.metadata()?.len();
    info!(
        "Dehydrated: {} ({:.5}%, {})",
        target_fn,
        ((delta_size as f64 / orig_size as f64) * 100f64),
        indicatif::HumanBytes(delta_size)
    );
    Ok(())
}

#[context("Creating rsync delta")]
fn rsync_delta(src: &Artifact, target: &Artifact, destdir: impl AsRef<Utf8Path>) -> Result<bool> {
    let destdir = destdir.as_ref();
    let src_fn = &get_maybe_uncompressed(src)?;
    let target_fn = &get_maybe_uncompressed(target)?;
    let delta_path = &destdir.join(rdelta_name_for_artifact(target)?);
    rsync_delta_impl(src_fn, target_fn, delta_path)?;
    Ok(true)
}

pub(crate) fn read_stream() -> Result<CoreStream> {
    let stream_path = Utf8Path::new(STREAM_FILE);
    let s = File::open(stream_path).context("Failed to open stream.json")?;
    let s: CoreStream = serde_json::from_reader(std::io::BufReader::new(s))?;
    Ok(s)
}

fn cached_uncompressed_name(a: &Artifact) -> Result<Option<(Utf8PathBuf, bool)>> {
    let name = a.filename();
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
    let name = Utf8Path::new(a.filename());
    let r = cached_uncompressed_name(a)?
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
                    info!("Converted to uncompressed qcow2: {}", name);
                } else {
                    std::fs::rename(&tmpname, &uncomp_name)?;
                }
                info!("Uncompressed: {}", uncomp_name);
            }
            Ok::<_, anyhow::Error>(uncomp_name)
        })
        .transpose()?
        .unwrap_or_else(|| name.into());
    Ok(r)
}

// Generate an image from its rsync delta.
fn dehydrate_rsyncable(qemu: &Artifact, target: &Artifact, destdir: &Utf8Path) -> Result<()> {
    let _found: bool = rsync_delta(qemu, target, destdir)?;
    Ok(())
}

// Special dehydration for OVAs.
fn dehydrate_ova(qemu: &Artifact, target: &Artifact, destdir: &Utf8Path) -> Result<()> {
    let ova_name = target.filename();
    let (ova_meta, tmp_delta) = {
        let mut temp_vmdk = tempfile::NamedTempFile::new_in(destdir)?;
        let ova_meta = ova::ova_extract(ova_name, &mut temp_vmdk)?;
        temp_vmdk.flush()?;
        let temp_vmdk = temp_vmdk.into_temp_path();
        let temp_vmdk: &Path = temp_vmdk.as_ref();
        let temp_vmdk_path: &Utf8Path = temp_vmdk.try_into()?;
        // Now decompress the VMDK
        let temp_qcow2 = tempfile::Builder::new()
            .prefix(ova_name)
            .tempfile_in(destdir)?
            .into_temp_path();
        let temp_qcow2: &Path = temp_qcow2.as_ref();
        let temp_qcow2: &Utf8Path = temp_qcow2.try_into()?;
        qemu_img::copy_to_qcow2(temp_vmdk_path, temp_qcow2)?;
        // Done with the vmdk
        drop(temp_vmdk);
        // And close the qcow2 fd
        let src_fn = &get_maybe_uncompressed(qemu)?;
        let tmp_delta = tempfile::NamedTempFile::new_in(destdir)?;
        let tmp_delta_path: &Utf8Path = tmp_delta.path().try_into()?;
        rsync_delta_impl(src_fn, temp_qcow2, tmp_delta_path)?;
        (ova_meta, tmp_delta)
    };
    let tmp_delta_path: &Utf8Path = tmp_delta.path().try_into()?;

    let ova_delta = ova_rdelta_name_for_artifact(target);
    let mut destf = BufWriter::new(File::create(destdir.join(&ova_delta))?);
    ova::ova_rebuild(&ova_meta, tmp_delta_path, &mut destf)?;
    destf.flush()?;
    info!("Generated delta OVA: {}", ova_delta);
    Ok(())
}

/// Loop over stream metadata and generate dehydrated (~deduplicated) content.
fn build_dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let stream_path = Utf8Path::new(STREAM_FILE);
    let s = read_stream()?;
    let riverdelta: RiverDelta = s.try_into()?;

    if !opts.allow_unhandled && !riverdelta.unhandled.is_empty() {
        return Err(anyhow!(
            "Unhandled artifacts: {:?}",
            riverdelta.unhandled.keys()
        ));
    }

    std::fs::create_dir_all(CACHEDIR).context("Creating cachedir")?;

    let qemu = &riverdelta.qemu;
    let uncomp_qemu = &get_maybe_uncompressed(qemu)?;
    let destdir = camino::Utf8Path::new(DIR);
    std::fs::create_dir(destdir)
        .with_context(|| anyhow!("Failed to create destination directory: {}", destdir))?;

    hardlink(stream_path, destdir.join(stream_path.file_name().unwrap()))?;

    if let Some(metal) = riverdelta.metal.as_ref() {
        // The rootfs (squashfs-in-cpio) is a source artifact for the ISO
        let rootfs_name = metal.pxe.rootfs.filename();
        hardlink(rootfs_name, destdir.join(rootfs_name))?;
        let _found: bool = rsync_delta(&metal.pxe.rootfs, &metal.iso, destdir)?;

        // And handle the kernel/initramfs
        for a in [&metal.pxe.kernel, &metal.pxe.initramfs].iter() {
            let name = a.filename();
            hardlink(name, destdir.join(name))?;
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
        riverdelta
            .qemu_rsyncable_artifacts
            .par_iter()
            .map(|(_name, target)| dehydrate_rsyncable(qemu, target, destdir))
            .chain(
                riverdelta
                    .aws
                    .par_iter()
                    .map(|aws| dehydrate_rsyncable(qemu, aws, destdir)),
            )
            .chain(
                riverdelta
                    .vmware
                    .par_iter()
                    .map(|vmware| dehydrate_ova(qemu, vmware, destdir)),
            )
            .try_reduce(|| (), |_, _| Ok(()))?;
        Ok::<_, anyhow::Error>(())
    })?;

    info!("Including (zstd compressed): {}", qemu_dest);
    zstd_compress(qemu_dest)?;

    let original_artifact_size = riverdelta.original_compressed_size()?;
    // TODO record exact filenames we expect
    let new_size = std::fs::read_dir(destdir)?
        .into_iter()
        .try_fold(0u64, |acc, f| {
            let f = f?;
            let l = if f.file_type()?.is_file() {
                f.metadata()?.len()
            } else {
                0
            };
            Ok::<_, anyhow::Error>(acc + l)
        })
        .context("Computing new size")?;

    // Write metadata JSON
    {
        let metadata = Metadata {
            original_artifact_size,
        };
        let w = std::io::BufWriter::new(File::create(destdir.join(METADATA_FILE))?);
        serde_json::to_writer_pretty(w, &metadata)?;
    }

    info!(
        "Original artifact total size: {}",
        indicatif::HumanBytes(original_artifact_size)
    );
    info!(
        "Dehydrated artifact total size: {}",
        indicatif::HumanBytes(new_size)
    );

    if !riverdelta.unhandled.is_empty() {
        assert!(!opts.allow_unhandled);
        let s = std::io::stdout();
        let mut s = s.lock();
        write!(s, "Unhandled:")?;
        for k in riverdelta.unhandled.keys() {
            write!(s, " {}", k)?;
        }
        writeln!(s, "")?;
    }

    Ok(())
}

fn main() {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{fmt, EnvFilter};

    // We need to write to stderr by default because a primary
    // use case is to output images to stdout.
    let fmt_layer = fmt::layer().with_writer(std::io::stderr);
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .with(fmt_layer)
        .init();
    // Print the error
    if let Err(e) = run() {
        tracing::error!("{:#}", e);
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
