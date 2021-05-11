//! A river delta is a fork of a stream.  Yes, I am clever at naming.
//! This module manages a "parsed" version of a stream that is
//! organized around how we manage deltas.

use anyhow::{anyhow, Result};
use coreos_stream_metadata::{Artifact, Platform, Stream};
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;

// openstack and ibmcloud are just qcow2 images.
// gcp is a tarball with a sparse disk image inside it, but for rsync that's
// not really different than a qcow2.
const RSYNC_STRATEGY_DISK: &[&str] = &["openstack", "ibmcloud", "gcp"];
pub(crate) const QEMU: &str = "qemu";
const METAL: &str = "metal";
const AWS: &str = "aws";

/// A parsed stream with data for the current CPU architecture,
/// split up by delta strategy.
pub(crate) struct RiverDelta {
    /// Name of the stream.
    pub stream: String,

    pub(crate) qemu_rsyncable_artifacts: HashMap<String, Artifact>,
    /// This is the VMDK, not the AMIs.
    pub(crate) aws: Option<Artifact>,
    pub(crate) unhandled: HashMap<String, Platform>,

    pub(crate) qemu: Artifact,
    pub(crate) metal: Option<Platform>,
}

impl RiverDelta {
    pub(crate) fn get_rsyncable(&self, name: &str) -> Option<&Artifact> {
        if name == AWS {
            return self.aws.as_ref();
        }
        self.qemu_rsyncable_artifacts.get(name)
    }
}

fn platform_disk_artifact(p: Platform) -> Result<Artifact> {
    p.formats
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Empty platform"))?
        .1
        .remove("disk")
        .ok_or_else(|| anyhow!("Missing 'disk' entry for platform"))
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
        let metal = thisarch.artifacts.remove(METAL);
        let aws = thisarch
            .artifacts
            .remove(AWS)
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
            metal,
            unhandled,
        })
    }
}
