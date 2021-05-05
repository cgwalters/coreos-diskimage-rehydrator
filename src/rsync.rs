//use rayon::prelude::*;
use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use serde_derive::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, Serialize, Deserialize)]
struct RollsumDeltaHeader {
    src_name: String,
    dest_name: String,
}

//fn reflink(src: impl AsRef<Utf8Path>, dest: impl AsRef<Utf8Path>) -> Result<()> {
//    let status = Command::new("cp")
//        .args(&["-p", "--reflink=auto"])
//        .arg(src.as_ref())
//        .arg(dest.as_ref())
//        .status()?;
//    if !status.success() {
//        return Err(anyhow!("cp --reflink failed: {:?}", status));
//    }
//    Ok(())
//}

fn setup_rsync_src(src: &Utf8Path, tempdir: &Utf8Path) -> Result<()> {
    let src_filename = src
        .file_name()
        .ok_or_else(|| anyhow!("Invalid source filename {}", src))?;
    let orig = "orig";
    let origdir = tempdir.join(orig);
    std::fs::create_dir(&origdir)?;
    std::fs::hard_link(src, origdir.join(src_filename)).context("Creating src hardlink")?;
    Ok(())
}

pub(crate) fn prepare(
    src: &Utf8Path,
    dest: &Utf8Path,
    tempdir: &Utf8Path,
    mut patch: impl Write,
) -> Result<()> {
    let tempdir = tempfile::tempdir_in(tempdir).context("Creating tempdir")?;
    let tempdir: &Utf8Path = tempdir.path().try_into()?;
    let src_filename = src
        .file_name()
        .ok_or_else(|| anyhow!("Invalid source filename {}", src))?;
    setup_rsync_src(src, tempdir)?;
    let destdir = tempdir.join("new");
    std::fs::create_dir(&destdir)?;
    std::fs::hard_link(dest, destdir.join(src_filename)).with_context(|| format!("Creating dest hardlink from {}", src_filename))?;
    let out: Utf8PathBuf = tempdir.join("d");
    println!("Preparing delta: {} -> {}", src, dest);
    let status = Command::new("rsync")
        .args(&["-rl"])
        .arg(format!("--only-write-batch={}", out.as_str()))
        .args(&["new/", "orig/"]) // FIXME I have no idea why these need to be (apparently) inverted
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .current_dir(tempdir)
        .status()?;
    if !status.success() {
        return Err(anyhow!("rsync failed: {:?}", status));
    }

    let batchf = File::open(&out).with_context(|| format!("Reading delta file {}", out))?;
    let mut batchf = std::io::BufReader::new(batchf);
    std::io::copy(&mut batchf, &mut patch)?;
    Ok(())
}

pub(crate) fn apply(
    src: &Utf8Path,
    dest_filename: &str,
    tempdir: &Utf8Path,
    patch: impl AsRef<Utf8Path>,
) -> Result<()> {
    let src_filename = src
        .file_name()
        .ok_or_else(|| anyhow!("Invalid source filename {}", src))?;

    let tempdir = tempfile::tempdir_in(tempdir).context("Creating tempdir")?;
    let tempdir: &Utf8Path = tempdir.path().try_into()?;

    let patch = {
        let patch = patch.as_ref();
        let mut patchin = zstd::Decoder::new(File::open(patch)?)?;
        let mut p = tempfile::NamedTempFile::new_in(tempdir)?;
        std::io::copy(&mut patchin, &mut p)?;
        p.flush()?;
        p
    };
    let patch: &Utf8Path = patch.path().try_into()?;

    let destdir = tempdir.join("new");
    let temp_dest = destdir.join(src_filename);
    std::fs::create_dir(&destdir)?;
    std::fs::hard_link(src, &temp_dest).context("Creating dest hardlink")?;

    println!("Rehydrating: {} -> {}", src, dest_filename);
    let status = Command::new("rsync")
        .args(&["-rl"])
        .arg(format!("--read-batch={}", patch))
        .args(&["new/"])
        .current_dir(tempdir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(anyhow!("rsync failed: {:?}", status));
    }
    std::fs::rename(&temp_dest, dest_filename)
        .with_context(|| anyhow!("Renaming {}", temp_dest))?;
    Ok(())
}
