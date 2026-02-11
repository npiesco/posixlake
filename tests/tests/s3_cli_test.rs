//! Integration tests for CLI S3 management commands
//!
//! Checks for podman/docker on native PATH first, then WSL.
//! The CLI itself does the same fallback, so one test covers both paths.

use std::process::Command;

fn posixlake_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // Remove test binary name
    path.pop(); // Remove deps
    path.push("posixlake");
    path
}

fn has_native_engine() -> bool {
    Command::new("podman")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        || Command::new("docker")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
}

#[cfg(target_os = "windows")]
fn has_wsl_engine() -> bool {
    let distro = std::env::var("WSL_DISTRO").unwrap_or_else(|_| "Ubuntu".to_string());
    let output = Command::new("C:\\Windows\\System32\\wsl.exe")
        .arg("-d")
        .arg(&distro)
        .arg("sh")
        .arg("-lc")
        .arg("command -v podman || command -v docker")
        .output();
    match output {
        Ok(out) => out.status.success() && !out.stdout.is_empty(),
        Err(_) => false,
    }
}

#[cfg(not(target_os = "windows"))]
fn has_wsl_engine() -> bool {
    false
}

/// s3 start/stop via compose should succeed (native or WSL engine)
#[test]
fn test_cli_s3_start_stop_compose() {
    if !has_native_engine() && !has_wsl_engine() {
        eprintln!("Skipping: no container engine found (native PATH or WSL)");
        return;
    }

    let output = Command::new(posixlake_binary())
        .arg("s3")
        .arg("start")
        .arg("--engine")
        .arg("podman")
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg("docker-compose.yml")
        .output()
        .expect("Failed to execute posixlake s3 start");

    assert!(
        output.status.success(),
        "s3 start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new(posixlake_binary())
        .arg("s3")
        .arg("stop")
        .arg("--engine")
        .arg("podman")
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg("docker-compose.yml")
        .output()
        .expect("Failed to execute posixlake s3 stop");

    assert!(
        output.status.success(),
        "s3 stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
