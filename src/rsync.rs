//use rayon::prelude::*;
use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use serde_derive::{Deserialize, Serialize};
use std::{convert::TryInto, io::Write};
use std::{
    fs::File,
    process::{Command, Stdio},
};

#[derive(Debug, Serialize, Deserialize)]
struct RollsumDeltaHeader {
    src_name: String,
    dest_name: String,
}

pub(crate) fn prepare(
    src: &camino::Utf8Path,
    dest: &camino::Utf8Path,
    tempdir: &camino::Utf8Path,
    mut patch: impl Write,
) -> Result<()> {
    let tempdir = tempfile::tempdir_in(tempdir).context("Creating tempdir")?;
    let tempdir: &Utf8Path = tempdir.path().try_into()?;
    let src_filename = src
        .file_name()
        .ok_or_else(|| anyhow!("Invalid source filename {}", src))?;
    let dest_filename = dest
        .file_name()
        .ok_or_else(|| anyhow!("Invalid destination filename {}", dest))?;
    let origdir = tempdir.join("orig");
    std::fs::create_dir(&origdir)?;
    std::fs::hard_link(src, origdir.join(src_filename)).context("Creating src hardlink")?;
    let newdir = tempdir.join("new");
    std::fs::create_dir(&newdir)?;
    std::fs::hard_link(dest, newdir.join(src_filename)).context("Creating dest hardlink")?;

    let out: Utf8PathBuf = tempdir.join("d");
    let status = Command::new("rsync")
        .arg("-a")
        .arg(format!("--only-write-batch={}", out.as_str()))
        .args(&[format!("{}/", origdir), format!("{}/", newdir)])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(anyhow!("rsync failed: {:?}", status));
    }

    let batchf = File::open(&out).with_context(|| format!("Reading delta file {}", out))?;
    let mut batchf = std::io::BufReader::new(batchf);
    std::io::copy(&mut batchf, &mut patch)?;
    Ok(())
}
