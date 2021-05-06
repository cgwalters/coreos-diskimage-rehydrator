use anyhow::{anyhow, Result};
use camino::Utf8Path;
use std::process::{Command, Stdio};

/// The ova file extension.
pub(crate) const OVA: &str = "ova";
/// Flags to create an ova tarball.  Copied from coreos-assembler.
pub(crate) const CREATE_FLAGS: &[&str] = &["--format=ustar"];

/// Extract an .ova file to a directory.
pub(crate) fn extract_ova(src: impl AsRef<Utf8Path>, dest: impl AsRef<Utf8Path>) -> Result<()> {
    let src = src.as_ref();
    let dest = dest.as_ref();

    std::fs::create_dir(dest)?;
    let s = Command::new("tar")
        .args(&["xf"])
        .args(&[src.as_str()])
        .stdout(Stdio::null())
        .current_dir(dest)
        .output()?;
    if !s.status.success() {
        return Err(anyhow!("tar failed: {}", s.status));
    }
    Ok(())
}

/// Create a .ova file from a directory
pub(crate) fn create_ova(src: impl AsRef<Utf8Path>, dest: impl AsRef<Utf8Path>) -> Result<()> {
    let src = src.as_ref();
    let dest = dest.as_ref();

    let s = Command::new("tar")
        .args(&["-ch", "-f", dest.as_str(), "-C", src.as_str(), "."])
        .args(CREATE_FLAGS)
        .stdout(Stdio::null())
        .output()?;
    if !s.status.success() {
        return Err(anyhow!("tar failed: {}", s.status));
    }
    Ok(())
}
