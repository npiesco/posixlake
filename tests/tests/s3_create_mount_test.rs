//! RED tests — `create` and `mount` must accept S3 URIs.
//!
//! These tests confirm the CLI handles `s3://` paths for `create` (and
//! eventually `mount`).  They require a running MinIO (ports 9000/9001).
//! The setup mirrors `s3_cli_test.rs`: build binary, check engine/ports,
//! skip gracefully when infrastructure is absent.

use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

// ── helpers (duplicated from s3_cli_test.rs to keep each file self-contained) ──

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
            "failed to build posixlake-cli\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        let binary_name = if cfg!(windows) {
            "posixlake-cli.exe"
        } else {
            "posixlake-cli"
        };
        let binary_path = target_dir.join("debug").join(binary_name);
        assert!(binary_path.exists(), "binary not found at {:?}", binary_path);
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
    debug_path.pop();
    debug_path.pop();
    debug_path.push("posixlake-cli");
    if cfg!(windows) && !debug_path.exists() {
        let mut exe = debug_path.clone();
        exe.set_extension("exe");
        if exe.exists() {
            return exe;
        }
    }
    if debug_path.exists() {
        return debug_path;
    }
    let mut release_path = std::env::current_exe().unwrap();
    release_path.pop();
    release_path.pop();
    release_path.pop();
    release_path.push("release");
    release_path.push("posixlake-cli");
    if cfg!(windows) && !release_path.exists() {
        let mut exe = release_path.clone();
        exe.set_extension("exe");
        if exe.exists() {
            return exe;
        }
    }
    if release_path.exists() {
        return release_path;
    }
    build_posixlake_binary().clone()
}

fn minio_is_ready() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().unwrap(),
        Duration::from_secs(2),
    )
    .is_ok()
}

fn unique_s3_path(test_name: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("s3://posixlake-test/{test_name}_{ts}")
}

// ── RED: create with S3 URI ──

/// `posixlake-cli create s3://bucket/path --schema col:Type` must succeed.
#[test]
fn test_create_with_s3_uri() {
    if !minio_is_ready() {
        eprintln!("Skipping: MinIO not reachable at http://localhost:9000");
        return;
    }

    let s3 = unique_s3_path("create_s3");
    let mut cmd = Command::new(posixlake_binary());
    cmd.arg("create")
        .arg(&s3)
        .arg("--schema")
        .arg("id:Int32,sensor:String,reading:Int32,location:String")
        .arg("--endpoint")
        .arg("http://localhost:9000")
        .arg("--access-key")
        .arg("minioadmin")
        .arg("--secret-key")
        .arg("minioadmin");

    let Some(output) = run_with_timeout(cmd, Duration::from_secs(60)) else {
        panic!("create with S3 URI timed out");
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "create with S3 URI failed (exit {}):\n{}",
        output.status,
        stderr,
    );
    assert!(
        stderr.contains("Database created successfully"),
        "expected success message in stderr, got:\n{}",
        stderr,
    );
}

/// `posixlake-cli create s3://bucket/path --schema ...` must accept S3 env vars
/// instead of explicit flags.
#[test]
fn test_create_with_s3_env_vars() {
    if !minio_is_ready() {
        eprintln!("Skipping: MinIO not reachable at http://localhost:9000");
        return;
    }

    let s3 = unique_s3_path("create_s3_env");
    let mut cmd = Command::new(posixlake_binary());
    cmd.arg("create")
        .arg(&s3)
        .arg("--schema")
        .arg("id:Int32,name:String")
        .env("AWS_ENDPOINT_URL", "http://localhost:9000")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin");

    let Some(output) = run_with_timeout(cmd, Duration::from_secs(60)) else {
        panic!("create with S3 env vars timed out");
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "create with S3 env vars failed (exit {}):\n{}",
        output.status,
        stderr,
    );
}

/// `posixlake-cli create s3://... --schema ...` followed by
/// `posixlake-cli health s3://...` must report "healthy".
#[test]
fn test_health_with_s3_uri() {
    if !minio_is_ready() {
        eprintln!("Skipping: MinIO not reachable at http://localhost:9000");
        return;
    }

    let s3 = unique_s3_path("health_s3");

    // create
    let mut create_cmd = Command::new(posixlake_binary());
    create_cmd
        .arg("create")
        .arg(&s3)
        .arg("--schema")
        .arg("id:Int32,val:String")
        .arg("--endpoint")
        .arg("http://localhost:9000")
        .arg("--access-key")
        .arg("minioadmin")
        .arg("--secret-key")
        .arg("minioadmin");
    let Some(create_out) = run_with_timeout(create_cmd, Duration::from_secs(60)) else {
        panic!("create timed out");
    };
    assert!(create_out.status.success(), "create failed");

    // health
    let mut health_cmd = Command::new(posixlake_binary());
    health_cmd
        .arg("health")
        .arg(&s3)
        .arg("--endpoint")
        .arg("http://localhost:9000")
        .arg("--access-key")
        .arg("minioadmin")
        .arg("--secret-key")
        .arg("minioadmin");
    let Some(health_out) = run_with_timeout(health_cmd, Duration::from_secs(30)) else {
        panic!("health timed out");
    };
    let stdout = String::from_utf8_lossy(&health_out.stdout);
    let stderr = String::from_utf8_lossy(&health_out.stderr);
    assert!(
        health_out.status.success(),
        "health with S3 URI failed (exit {}):\nstdout:{}\nstderr:{}",
        health_out.status,
        stdout,
        stderr,
    );
    assert!(
        stdout.contains("healthy") || stdout.contains("status"),
        "expected health output, got:\n{}",
        stdout,
    );
}
