use anyhow::{anyhow, Result};
use camino::Utf8Path;
use std::process::{Command, Stdio};

pub(crate) const QCOW2: &str = "qcow2";
pub(crate) const VMDK: &str = "vmdk";
/// Options for qemu-img to make our vmdk, taken from coreos-assembler
// TODO inspect the vmdk to find this?  At least `streamOptimized` is in the
// output from `qemu-img info --output=json` but the other parts arent.
const VMDK_OPTS: &str = "adapter_type=lsilogic,subformat=streamOptimized,compat6";

/// Copy and convert a image (e.g. `.vmdk`) to an uncompressed qcow2
pub(crate) fn copy_to_qcow2(p: impl AsRef<Utf8Path>, dest: impl AsRef<Utf8Path>) -> Result<()> {
    let p = p.as_ref();
    let dest = dest.as_ref();

    let s = Command::new("qemu-img")
        .args(&["convert", "-q", "-f", VMDK, "-O", QCOW2])
        .args(&[p.as_str(), dest.as_str()])
        .stdout(Stdio::null())
        .output()?;
    if !s.status.success() {
        return Err(anyhow!("qemu-img failed: {}", s.status));
    }
    Ok(())
}

/// Copy and convert a `.qcow2 image to a stream-optimized VMDK
pub(crate) fn copy_to_vmdk(p: impl AsRef<Utf8Path>, dest: impl AsRef<Utf8Path>) -> Result<()> {
    let p = p.as_ref();
    let dest = dest.as_ref();

    let s = Command::new("qemu-img")
        .args(&["convert", "-q", "-f", QCOW2, "-O", VMDK])
        .args(&["-o", VMDK_OPTS])
        .args(&[p.as_str(), dest.as_str()])
        .stdout(Stdio::null())
        .output()?;
    if !s.status.success() {
        return Err(anyhow!("qemu-img failed: {}", s.status));
    }
    Ok(())
}
