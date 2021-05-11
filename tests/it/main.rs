use anyhow::Result;
use assert_cmd::prelude::*;
use std::process::{Command, Stdio};

#[test]
fn test_run_help() -> Result<()> {
    let mut cmd = Command::cargo_bin("coreos-diskimage-rehydrator")?;
    cmd.arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let s = cmd.status()?;
    assert!(s.success());
    Ok(())
}
