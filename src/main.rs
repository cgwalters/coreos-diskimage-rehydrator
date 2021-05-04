use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use coreos_stream_metadata::Artifact;
use rayon::prelude::*;
use std::{collections::HashSet, convert::TryInto};
use std::fs::File;
use std::io::Write;
use structopt::StructOpt;

#[deny(unused_must_use)]
mod rsync;

// Number of CPUs we'll use
const N_WORKERS: u32 = 2;
const RSYNC_STRATEGY: &[&str] = &["openstack", "ibmcloud", "gcp"];

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
    
    #[structopt(long)]
    skip_unavailable: bool,
}

#[derive(Debug, StructOpt)]
struct RehydrateOpts {
    /// Path to qemu image
    src_qemu: String,

    patch: String,

    output: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "coreos-diskimage-rehydrator")]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    /// Generate "dehydration files"
    Dehydrate(DehydrateOpts),
    /// Regenerate target file
    Rehydrate(RehydrateOpts),
}

fn run() -> Result<()> {
    match Opt::from_args() {
        Opt::Dehydrate(ref opts) => dehydrate(opts),
        Opt::Rehydrate(ref opts) => rehydrate(opts),
    }
}

fn rehydrate(opts: &RehydrateOpts) -> Result<(), anyhow::Error> {
    todo!()
}

fn uncompressed_name(s: &str) -> &str {
    s.strip_suffix(".xz")
        .or_else(|| s.strip_suffix(".gz"))
        .unwrap_or(s)
}

fn filename_for_artifact(a: &Artifact) -> Result<&str> {
    Ok(Utf8Path::new(&a.location)
        .file_name()
        .ok_or_else(|| anyhow!("Invalid artifact location: {}", a.location))?)
}

fn dehydrate(opts: &DehydrateOpts) -> Result<()> {
    let s = File::open("stream.json").context("Failed to open stream.json")?;
    let s: coreos_stream_metadata::Stream = serde_json::from_reader(std::io::BufReader::new(s))?;
    let thisarch = s.this_architecture()
        .ok_or_else(|| anyhow!("Missing this architecture in stream metadata"))?;
    let qemu = s
        .query_thisarch_single("qemu")
        .ok_or_else(|| anyhow!("Missing qemu image"))?;
    let qemu_fn = filename_for_artifact(qemu)?;
    let qemu_fn = Utf8Path::new(uncompressed_name(qemu_fn));
    if !qemu_fn.exists() {
        return Err(anyhow!("Missing uncompressed qemu image: {}", qemu_fn));
    }
    let destdir = camino::Utf8Path::new("dehydrated");
    std::fs::create_dir(destdir)
        .with_context(|| anyhow!("Failed to create destination directory: {}", destdir))?;
    std::fs::hard_link(qemu_fn, destdir.join(qemu_fn)).context("Hardlinking qemu image")?;

    let rsyncable: HashSet<_> = RSYNC_STRATEGY.iter().collect();
    let rsyncable: Vec<&Artifact> = thisarch.artifacts.iter().filter_map(|(k, v)| if rsyncable.contains(&k.as_str()) {
        Some(v.formats.get("disk").unwrap())
    } else {
        None
    }).collect();

    let platforms: Vec<&str> = 
        s.this_architecture()
            .unwrap()
            .artifacts
            .keys()
            .map(|s| s.as_str())
            .collect();


    let metal = thisarch.artifacts.get("metal")
        .ok_or_else(||anyhow!("Missing metal artifacts"))?;
    let rsyncable: Vec<_> = artifacts.iter().filter(|a| rsyncable.contains(a)).collect();
    rsyncable.push(metal.formats.get("raw.xz"));

    println!("Processing artifacts: {}", artifacts.join(" "));

    // Add some parallelism
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(N_WORKERS as usize)
        .build()
        .unwrap();
    pool.install(|| -> Result<_> {
        let platforms = &["openstack", "gcp"];
        platforms.par_iter().try_for_each(|artifact| {
            let a = s
                .query_thisarch_single(artifact)
                .ok_or_else(|| anyhow!("Missing artifact {}", artifact))?;
            let orig_fn = filename_for_artifact(a)?;
            let orig_fn = Utf8Path::new(uncompressed_name(orig_fn));
            if !orig_fn.exists() {
                if opts.skip_unavailable {
                    println!("Skipping: {}", orig_fn);
                    return Ok(());
                }
                return Err(anyhow!("Missing image: {}", orig_fn));
            }
            let delta_path = &destdir.join(format!("{}.rdelta", orig_fn));
            let mut output = std::io::BufWriter::new(File::create(delta_path)?);
            rsync::prepare(qemu_fn, orig_fn, destdir, &mut output)?;
            output.flush()?;
            let orig_size = orig_fn.metadata()?.len() / (1000 * 1000);
            let delta_size = delta_path.metadata()?.len() / (1000 * 1000);
            println!(
                "Dehydrated: {} ({}%, {} MB)",
                orig_fn,
                ((delta_size as f64 / orig_size as f64) * 100f64).trunc() as u32,
                delta_size
            );
            Ok(())
        })
    })?;


    let metal = if let Some(metal) = metal {
        metal
    } else {
        if opts.skip_unavailable {
            println!("Skipping metal artifacts")
        }
    }

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
