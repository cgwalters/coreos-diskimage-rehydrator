use crate::riverdelta::{ArtifactExt, RiverDelta};
use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use rayon::prelude::*;
use smallvec::SmallVec;
use std::convert::TryInto;
use std::fs::File;
use tracing::info;

pub(crate) fn build_download() -> Result<()> {
    let s = crate::read_stream()?;
    let riverdelta: RiverDelta = s.try_into()?;
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
        artifacts.push(&riverdelta.qemu);
        artifacts.extend(riverdelta.aws.as_ref().iter());
        artifacts.extend(
            riverdelta
                .qemu_rsyncable_artifacts
                .iter()
                .map(|(_name, v)| v),
        );
        if let Some(metal) = riverdelta.metal.as_ref() {
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
        .num_threads(crate::N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| -> Result<_> {
        artifacts
            .par_iter()
            .flat_map_iter(|&a| {
                let mut r = SmallVec::<[(&str, &Utf8Path); 2]>::new();
                let img_fname = Utf8Path::new(a.filename());
                if !img_fname.exists() {
                    r.push((a.location.as_str(), img_fname))
                }
                if let Some(signature) = a.signature.as_deref() {
                    let sig_fname: &Utf8Path = Utf8Path::new(signature).file_name().unwrap().into();
                    if !sig_fname.exists() {
                        r.push((signature, sig_fname))
                    }
                }
                r
            })
            .try_for_each_init(
                || client.clone(),
                |client, (location, fname)| -> Result<()> {
                    let temp_name = &format!("{}.tmp", fname);
                    let mut out = std::io::BufWriter::new(File::create(temp_name)?);
                    let mut resp = client.get(location).send()?;
                    resp.copy_to(&mut out)
                        .with_context(|| anyhow!("Failed to download {}", location))?;
                    std::fs::rename(temp_name, fname)?;
                    info!("Downloaded: {}", fname);
                    Ok(())
                },
            )
    })?;
    let size: u64 = artifacts
        .par_iter()
        .try_fold(
            || 0u64,
            |acc, &artifact| {
                let artifact_size = Utf8Path::new(artifact.filename()).metadata()?.len();
                Ok::<_, anyhow::Error>(acc + artifact_size)
            },
        )
        .try_reduce(|| 0u64, |a, b| Ok(a + b))?;
    info!(
        "Original artifact total size: {}",
        indicatif::HumanBytes(size)
    );
    Ok(())
}
