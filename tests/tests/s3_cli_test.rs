//! Integration tests for CLI S3 management commands
//!
//! Checks for podman/docker on native PATH first, then WSL.
//! The CLI itself does the same fallback, so one test covers both paths.

use std::process::Command;

fn compose_file_path() -> std::path::PathBuf {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate should have workspace root parent")
        .join("docker-compose.yml");
    assert!(path.exists(), "docker-compose.yml not found at {:?}", path);
    path
}

fn posixlake_binary() -> std::path::PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_posixlake_cli") {
        let env_path = std::path::PathBuf::from(path);
        if env_path.exists() {
            return env_path;
        }
    }

    let mut debug_path = std::env::current_exe().unwrap();
    debug_path.pop(); // Remove test binary name
    debug_path.pop(); // Remove deps
    debug_path.push("posixlake-cli");
    if cfg!(windows) && !debug_path.exists() {
        let mut exe_path = debug_path.clone();
        exe_path.set_extension("exe");
        if exe_path.exists() {
            return exe_path;
        }
    }
    if debug_path.exists() {
        return debug_path;
    }

    let mut release_path = std::env::current_exe().unwrap();
    release_path.pop(); // Remove test binary name
    release_path.pop(); // Remove deps
    release_path.pop(); // Remove debug or release
    release_path.push("release");
    release_path.push("posixlake-cli");
    if cfg!(windows) && !release_path.exists() {
        let mut exe_path = release_path.clone();
        exe_path.set_extension("exe");
        if exe_path.exists() {
            return exe_path;
        }
    }

    assert!(
        release_path.exists(),
        "posixlake-cli binary not found. Tried debug path {:?} and release path {:?}. \
Build it first or set CARGO_BIN_EXE_posixlake_cli.",
        debug_path,
        release_path
    );
    release_path
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

fn selected_engine() -> Option<&'static str> {
    if Command::new("podman")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Some("podman");
    }
    if Command::new("docker")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Some("docker");
    }
    None
}

fn has_required_s3_images(engine: &str) -> bool {
    let check_cmd = match engine {
        "podman" => (
            "podman",
            vec!["image", "exists", "minio/minio:latest"],
            vec!["image", "exists", "minio/mc:latest"],
        ),
        "docker" => (
            "docker",
            vec!["image", "inspect", "minio/minio:latest"],
            vec!["image", "inspect", "minio/mc:latest"],
        ),
        _ => return false,
    };

    let minio = Command::new(check_cmd.0)
        .args(check_cmd.1)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    let mc = Command::new(check_cmd.0)
        .args(check_cmd.2)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    minio && mc
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
    let Some(engine) = selected_engine() else {
        eprintln!("Skipping: no native engine selected");
        return;
    };
    if !has_required_s3_images(engine) {
        eprintln!(
            "Skipping: required local images missing for {} (minio/minio:latest, minio/mc:latest)",
            engine
        );
        return;
    }

    let compose_file = compose_file_path();

    let _ = Command::new(posixlake_binary())
        .arg("s3")
        .arg("stop")
        .arg("--engine")
        .arg(engine)
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg(&compose_file)
        .output();

    let output = Command::new(posixlake_binary())
        .arg("s3")
        .arg("start")
        .arg("--engine")
        .arg(engine)
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg(&compose_file)
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
        .arg(engine)
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg(&compose_file)
        .output()
        .expect("Failed to execute posixlake s3 stop");

    assert!(
        output.status.success(),
        "s3 stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// s3-test should auto-start MinIO and create bucket if needed
#[test]
fn test_cli_s3_test_auto_start() {
    if !has_native_engine() && !has_wsl_engine() {
        eprintln!("Skipping: no container engine found (native PATH or WSL)");
        return;
    }
    let Some(engine) = selected_engine() else {
        eprintln!("Skipping: no native engine selected");
        return;
    };
    if !has_required_s3_images(engine) {
        eprintln!(
            "Skipping: required local images missing for {} (minio/minio:latest, minio/mc:latest)",
            engine
        );
        return;
    }

    let compose_file = compose_file_path();

    let _ = Command::new(posixlake_binary())
        .arg("s3")
        .arg("stop")
        .arg("--engine")
        .arg(engine)
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg(&compose_file)
        .output();

    let output = Command::new(posixlake_binary())
        .arg("s3-test")
        .arg("s3://posixlake-test/.posixlake")
        .output()
        .expect("Failed to execute posixlake s3-test");

    assert!(
        output.status.success(),
        "s3-test failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
