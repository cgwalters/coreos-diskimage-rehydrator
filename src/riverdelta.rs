//! A river delta is a fork of a stream.  Yes, I am clever at naming.
//! This module manages a "parsed" version of a stream that is
//! organized around how we manage deltas.

use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use coreos_stream_metadata::{Artifact, Platform, Stream};
use fn_error_context::context;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;

// Most of these are just just qcow2 images.
// gcp is a tarball with a sparse disk image inside it, but for rsync that's
// not really different than a qcow2.
// A few others are raw disk images (azure, vultr).
const RSYNC_STRATEGY_DISK: &[&str] = &[
    "aliyun",
    "azure",
    "exoscale",
    "openstack",
    "ibmcloud",
    "gcp",
    "digitalocean",
    "vultr",
];
pub(crate) const QEMU: &str = "qemu";
const METAL: &str = "metal";
const AWS: &str = "aws";
const VMWARE: &str = "vmware";

/// Extension trait for Artifact.
pub(crate) trait ArtifactExt {
    fn filename(&self) -> &str;
}

pub(crate) struct MetalPXE {
    pub(crate) kernel: Artifact,
    pub(crate) initramfs: Artifact,
    pub(crate) rootfs: Artifact,
}

pub(crate) struct Metal {
    pub(crate) iso: Artifact,
    pub(crate) pxe: MetalPXE,
}

/// A parsed stream with data for the current CPU architecture,
/// split up by delta strategy.
pub(crate) struct RiverDelta {
    /// Name of the stream.
    pub stream: String,

    /// Used as a basis for the qemu_rsyncable_artifact set.
    pub(crate) qemu: Artifact,
    /// Images which derive from qemu.
    pub(crate) qemu_rsyncable_artifacts: HashMap<String, Artifact>,
    /// This is the VMDK, not the AMIs.
    pub(crate) aws: Option<Artifact>,
    /// vmware image is an OVA, which needs special handling.
    pub(crate) vmware: Option<Artifact>,
    /// The Live ISO and PXE data
    pub(crate) metal: Option<Metal>,
    /// Unhandled set.
    pub(crate) unhandled: HashMap<String, Platform>,
}

impl RiverDelta {
    pub(crate) fn get_rsyncable(&self, name: &str) -> Option<&Artifact> {
        if name == AWS {
            return self.aws.as_ref();
        }
        self.qemu_rsyncable_artifacts.get(name)
    }

    /// Get all artifacts.
    pub(crate) fn all_artifacts(&self) -> Vec<&Artifact> {
        use std::iter::once;
        once(&self.qemu)
            .chain(self.qemu_rsyncable_artifacts.values())
            .chain(self.aws.as_ref())
            .chain(self.vmware.as_ref())
            .chain(
                self.metal
                    .iter()
                    .flat_map(|p| vec![&p.iso, &p.pxe.kernel, &p.pxe.initramfs, &p.pxe.rootfs]),
            )
            .collect()
    }

    /// Size in bytes of the original artifacts (compressed).
    #[context("Computing original compressed size")]
    pub(crate) fn original_compressed_size(&self) -> Result<u64> {
        let r = self
            .all_artifacts()
            .into_par_iter()
            .map(|a| Utf8Path::new(a.filename()))
            .try_fold(
                || 0u64,
                |acc, filename| {
                    let artifact_size = filename
                        .metadata()
                        .with_context(|| anyhow!("Finding metadata for {}", filename))?
                        .len();
                    Ok::<_, anyhow::Error>(acc + artifact_size)
                },
            )
            .try_reduce(|| 0u64, |a, b| Ok(a + b))?;
        Ok(r)
    }
}

/// Remove all signatures from a stream.
pub(crate) fn stream_remove_signatures(s: &mut Stream) -> Result<()> {
    let utsname = nix::sys::utsname::uname();
    let thisarch_name = utsname.machine();
    let thisarch = s
        .architectures
        .get_mut(thisarch_name)
        .ok_or_else(|| anyhow::anyhow!("Missing this architecture in stream metadata"))?;
    for platform in thisarch.artifacts.values_mut() {
        for format in platform.formats.values_mut() {
            for artifact in format.values_mut() {
                artifact.signature = None;
            }
        }
    }
    Ok(())
}

fn validate_artifact(a: Artifact) -> Result<Artifact> {
    if Utf8Path::new(a.location.as_str()).file_name().is_none() {
        return Err(anyhow!("Missing filename in {}", a.location));
    }
    Ok(a)
}

fn platform_disk_artifact(p: Platform) -> Result<Artifact> {
    let a = p
        .formats
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Empty platform"))?
        .1
        .remove("disk")
        .ok_or_else(|| anyhow!("Missing 'disk' entry for platform"))?;
    validate_artifact(a)
}

impl TryFrom<Stream> for RiverDelta {
    type Error = anyhow::Error;

    fn try_from(mut s: Stream) -> Result<Self, Self::Error> {
        let stream_name = s.stream;
        let utsname = nix::sys::utsname::uname();
        let thisarch_name = utsname.machine();
        let mut thisarch = s
            .architectures
            .remove(thisarch_name)
            .ok_or_else(|| anyhow::anyhow!("Missing this architecture in stream metadata"))?;
        let qemu = thisarch
            .artifacts
            .remove(QEMU)
            .ok_or_else(|| anyhow!("Missing qemu"))?;
        let qemu = platform_disk_artifact(qemu)?;
        let metal = thisarch
            .artifacts
            .remove(METAL)
            .map(|mut m| {
                let iso = m
                    .formats
                    .remove("iso")
                    .ok_or_else(|| anyhow!("metal missing `iso`"))?
                    .remove("disk")
                    .ok_or_else(|| anyhow!("metal/iso missing disk"))?;
                let mut pxe = m
                    .formats
                    .remove("pxe")
                    .ok_or_else(|| anyhow!("metal missing `pxe`"))?;
                let kernel = pxe
                    .remove("kernel")
                    .ok_or_else(|| anyhow!("metal/pxe missing kernel"))?;
                let initramfs = pxe
                    .remove("initramfs")
                    .ok_or_else(|| anyhow!("metal/pxe missing initramfs"))?;
                let rootfs = pxe
                    .remove("rootfs")
                    .ok_or_else(|| anyhow!("metal/pxe missing rootfs"))?;
                let pxe = MetalPXE {
                    kernel,
                    initramfs,
                    rootfs,
                };
                Ok::<_, anyhow::Error>(Metal { iso, pxe })
            })
            .transpose()?;
        let aws = thisarch
            .artifacts
            .remove(AWS)
            .map(platform_disk_artifact)
            .transpose()?;
        let vmware = thisarch
            .artifacts
            .remove(VMWARE)
            .map(platform_disk_artifact)
            .transpose()?;
        let (qemu_rsyncable_artifacts, unhandled): (HashMap<_, _>, HashMap<_, _>) = thisarch
            .artifacts
            .into_par_iter()
            .partition(|(name, _)| RSYNC_STRATEGY_DISK.contains(&name.as_str()));
        let qemu_rsyncable_artifacts: Result<HashMap<_, _>> = qemu_rsyncable_artifacts
            .into_iter()
            .map(|(k, v)| {
                let a = platform_disk_artifact(v)?;
                Ok::<_, anyhow::Error>((k, a))
            })
            .collect();
        let qemu_rsyncable_artifacts = qemu_rsyncable_artifacts?;
        Ok(RiverDelta {
            stream: stream_name,
            qemu,
            qemu_rsyncable_artifacts,
            aws,
            vmware,
            metal,
            unhandled,
        })
    }
}

impl ArtifactExt for Artifact {
    // Return the filename for the artifact.
    //
    // This was validated at parse time for a RiverDelta.
    fn filename(&self) -> &str {
        Utf8Path::new(&self.location).file_name().unwrap()
    }
}
