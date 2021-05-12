//! A river delta is a fork of a stream.  Yes, I am clever at naming.
//! This module manages a "parsed" version of a stream that is
//! organized around how we manage deltas.

use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use fn_error_context::context;
use std::convert::TryInto;
use std::fs::File;
use std::io::{BufReader, Write};

pub(crate) struct OVA {
    pub(crate) config: Vec<u8>,
    pub(crate) config_header: tar::Header,
    pub(crate) disk_header: tar::Header,
}

#[context("Extracting ova")]
pub(crate) fn ova_extract(src: impl AsRef<Utf8Path>, mut disk_dest: impl Write) -> Result<OVA> {
    let src = src.as_ref();
    let r = BufReader::new(File::open(src).with_context(|| anyhow!("Opening {}", src))?);
    let mut r = tar::Archive::new(r);
    let mut config = Vec::new();
    let mut config_header: Option<tar::Header> = None;
    let mut disk_header: Option<tar::Header> = None;
    for ent in r.entries()? {
        let mut ent = ent?;
        let name = ent.path()?;
        let name: &Utf8Path = (*name).try_into()?;
        match name.extension() {
            Some("ovf") => {
                if config_header.is_some() {
                    return Err(anyhow!("Found multiple ovf entries, second={}", name));
                }
                config_header = Some(ent.header().clone());
                std::io::copy(&mut ent, &mut config)?;
            }
            Some("vmdk") => {
                if disk_header.is_some() {
                    return Err(anyhow!("Found multiple vmdk entries, second={}", name));
                }
                disk_header = Some(ent.header().clone());
                std::io::copy(&mut ent, &mut disk_dest)?;
            }
            _ => return Err(anyhow!("Unhandled ova file {}", name)),
        }
    }
    let config_header = config_header.ok_or_else(|| anyhow!("failed to find ovf entry"))?;
    let disk_header = disk_header.ok_or_else(|| anyhow!("failed to find vmdk entry"))?;
    Ok(OVA {
        config_header,
        config,
        disk_header,
    })
}

/// Create a new ustar header derived from values in the original header.
fn header_clone_ustar(h: &tar::Header) -> Result<tar::Header> {
    let mut n = tar::Header::new_ustar();
    n.set_path(h.path()?)?;
    n.set_entry_type(h.entry_type());
    n.set_mode(h.mode()?);
    n.set_size(h.size()?);
    n.set_mtime(h.mtime()?);
    n.set_size(h.size()?);
    if let Some(u) = h.username()? {
        n.set_username(u)?;
    }
    if let Some(u) = h.groupname()? {
        n.set_groupname(u)?;
    }
    Ok(n)
}

#[context("Building ova")]
pub(crate) fn ova_rebuild(
    header: &OVA,
    disk: impl AsRef<Utf8Path>,
    dest: impl Write,
) -> Result<()> {
    let disk = disk.as_ref();
    let diskmeta = &disk.metadata()?;
    let mut builder = tar::Builder::new(dest);
    let mut config_header = header_clone_ustar(&header.config_header)?;
    config_header.set_cksum();
    builder.append(&config_header, header.config.as_slice())?;
    let mut disk_header = header_clone_ustar(&header.disk_header)?;
    disk_header.set_size(diskmeta.len());
    disk_header.set_cksum();
    let mut disk_src = BufReader::new(File::open(disk)?);
    builder.append(&disk_header, &mut disk_src)?;
    builder.into_inner()?;
    Ok(())
}
