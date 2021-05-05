use anyhow::{anyhow, Result};
use camino::Utf8Path;
use std::process::{Command, Stdio};

pub(crate) fn sha256_file(p: impl AsRef<Utf8Path>) -> Result<String> {
    let p = p.as_ref();
    let s = Command::new("sha256sum")
        .arg(p)
        .stdout(Stdio::piped())
        .output()?;
    if !s.status.success() {
        return Err(anyhow!("sha256sum failed: {}", s.status));
    }
    let stdout = std::str::from_utf8(&s.stdout)?;
    Ok(stdout.split_whitespace().next().unwrap().to_string())
}
