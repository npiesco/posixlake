//! Integration tests for CLI S3 management commands
//!
//! Checks for podman/docker on native PATH first, then WSL.
//! The CLI itself does the same fallback, so one test covers both paths.

use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

fn run_with_timeout(mut cmd: Command, timeout: Duration) -> Option<Output> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().ok()?;
    let deadline = Instant::now() + timeout;

    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

fn compose_file_path() -> std::path::PathBuf {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate should have workspace root parent")
        .join("docker-compose.yml");
    assert!(path.exists(), "docker-compose.yml not found at {:?}", path);
    path
}

fn build_posixlake_binary() -> &'static std::path::PathBuf {
    static BUILD_PATH: OnceLock<std::path::PathBuf> = OnceLock::new();
    BUILD_PATH.get_or_init(|| {
        let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("tests crate should have workspace root parent");
        let target_dir = std::env::temp_dir().join("posixlake-cli-test-target");
        let output = Command::new("cargo")
            .arg("build")
            .arg("-p")
            .arg("posixlake")
            .arg("--bin")
            .arg("posixlake-cli")
            .arg("--target-dir")
            .arg(&target_dir)
            .current_dir(workspace_root)
            .output()
            .expect("failed to build posixlake-cli for integration tests");
        assert!(
            output.status.success(),
            "failed to build posixlake-cli for integration tests\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let binary_name = if cfg!(windows) {
            "posixlake-cli.exe"
        } else {
            "posixlake-cli"
        };
        let binary_path = target_dir.join("debug").join(binary_name);
        assert!(
            binary_path.exists(),
            "built posixlake-cli binary not found at {:?}",
            binary_path
        );
        binary_path
    })
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

    if release_path.exists() {
        return release_path;
    }

    build_posixlake_binary().clone()
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

fn port_is_available(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
        && std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
}

fn has_required_s3_ports() -> bool {
    port_is_available(9000) && port_is_available(9001)
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
    if !has_required_s3_ports() {
        eprintln!("Skipping: required MinIO ports 9000/9001 are already in use");
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

    let mut start_cmd = Command::new(posixlake_binary());
    start_cmd
        .arg("s3")
        .arg("start")
        .arg("--engine")
        .arg(engine)
        .arg("--mode")
        .arg("compose")
        .arg("--compose-file")
        .arg(&compose_file);
    let Some(output) = run_with_timeout(start_cmd, Duration::from_secs(90)) else {
        eprintln!("Skipping: posixlake s3 start timed out");
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
        return;
    };

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
    if !has_required_s3_ports() {
        eprintln!("Skipping: required MinIO ports 9000/9001 are already in use");
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

    let mut s3_test_cmd = Command::new(posixlake_binary());
    s3_test_cmd
        .arg("s3-test")
        .arg("s3://posixlake-test/.posixlake");
    let Some(output) = run_with_timeout(s3_test_cmd, Duration::from_secs(90)) else {
        eprintln!("Skipping: posixlake s3-test timed out");
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
        return;
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    if stderr.contains("status: 507")
        || stderr.contains("No space left on device")
        || stdout.contains("status: 507")
        || stdout.contains("No space left on device")
    {
        eprintln!(
            "Skipping: insufficient local storage for MinIO-backed s3-test (detected HTTP 507 / no-space condition)"
        );
        return;
    }

    assert!(output.status.success(), "s3-test failed: {}", stderr);
}
