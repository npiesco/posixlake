//! Integration tests for CLI Azure management commands
//!
//! Tests the CLI's ability to create, health-check, and test databases
//! on Azure Blob Storage (Azurite) via `az://` URIs.
//! Mirrors `s3_create_mount_test.rs` for Azure.

use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const AZURITE_ACCOUNT_NAME: &str = "devstoreaccount1";
const AZURITE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

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
        assert!(
            binary_path.exists(),
            "binary not found at {:?}",
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

fn azurite_is_ready() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:10000".parse().unwrap(),
        Duration::from_secs(2),
    )
    .is_ok()
}

fn unique_azure_path(test_name: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("az://posixlake-test/{test_name}_{ts}")
}

/// `posixlake-cli create az://container/path --schema col:Type` must succeed
/// with explicit Azure flags.
#[test]
fn test_create_with_azure_uri() {
    if !azurite_is_ready() {
        eprintln!("Skipping: Azurite not reachable at http://127.0.0.1:10000");
        return;
    }

    let az = unique_azure_path("create_az");
    let mut cmd = Command::new(posixlake_binary());
    cmd.arg("create")
        .arg(&az)
        .arg("--schema")
        .arg("id:Int32,sensor:String,reading:Int32,location:String")
        .arg("--azure-account")
        .arg(AZURITE_ACCOUNT_NAME)
        .arg("--azure-key")
        .arg(AZURITE_ACCOUNT_KEY)
        .arg("--azure-endpoint")
        .arg("http://127.0.0.1:10000");

    let Some(output) = run_with_timeout(cmd, Duration::from_secs(60)) else {
        panic!("create with Azure URI timed out");
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "create with Azure URI failed (exit {}):\n{}",
        output.status,
        stderr,
    );
    assert!(
        stderr.contains("Database created successfully"),
        "expected success message in stderr, got:\n{}",
        stderr,
    );
}

/// `posixlake-cli create az://container/path --schema ...` must accept
/// Azure env vars instead of explicit flags.
#[test]
fn test_create_with_azure_env_vars() {
    if !azurite_is_ready() {
        eprintln!("Skipping: Azurite not reachable at http://127.0.0.1:10000");
        return;
    }

    let az = unique_azure_path("create_az_env");
    let mut cmd = Command::new(posixlake_binary());
    cmd.arg("create")
        .arg(&az)
        .arg("--schema")
        .arg("id:Int32,name:String")
        .env("AZURE_STORAGE_ACCOUNT_NAME", AZURITE_ACCOUNT_NAME)
        .env("AZURE_STORAGE_ACCOUNT_KEY", AZURITE_ACCOUNT_KEY)
        .env("AZURITE_ENDPOINT", "http://127.0.0.1:10000");

    let Some(output) = run_with_timeout(cmd, Duration::from_secs(60)) else {
        panic!("create with Azure env vars timed out");
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "create with Azure env vars failed (exit {}):\n{}",
        output.status,
        stderr,
    );
}

/// Create then health-check an Azure database via the CLI.
#[test]
fn test_health_with_azure_uri() {
    if !azurite_is_ready() {
        eprintln!("Skipping: Azurite not reachable at http://127.0.0.1:10000");
        return;
    }

    let az = unique_azure_path("health_az");

    // create
    let mut create_cmd = Command::new(posixlake_binary());
    create_cmd
        .arg("create")
        .arg(&az)
        .arg("--schema")
        .arg("id:Int32,name:String")
        .arg("--azure-account")
        .arg(AZURITE_ACCOUNT_NAME)
        .arg("--azure-key")
        .arg(AZURITE_ACCOUNT_KEY)
        .arg("--azure-endpoint")
        .arg("http://127.0.0.1:10000");

    let Some(create_out) = run_with_timeout(create_cmd, Duration::from_secs(60)) else {
        panic!("create timed out");
    };
    assert!(
        create_out.status.success(),
        "create failed:\n{}",
        String::from_utf8_lossy(&create_out.stderr),
    );

    // health
    let mut health_cmd = Command::new(posixlake_binary());
    health_cmd
        .arg("health")
        .arg(&az)
        .arg("--azure-account")
        .arg(AZURITE_ACCOUNT_NAME)
        .arg("--azure-key")
        .arg(AZURITE_ACCOUNT_KEY)
        .arg("--azure-endpoint")
        .arg("http://127.0.0.1:10000");

    let Some(health_out) = run_with_timeout(health_cmd, Duration::from_secs(60)) else {
        panic!("health timed out");
    };

    let stdout = String::from_utf8_lossy(&health_out.stdout);
    assert!(
        health_out.status.success(),
        "health failed:\n{}",
        String::from_utf8_lossy(&health_out.stderr),
    );
    assert!(
        stdout.contains("healthy") || stdout.contains("status"),
        "expected health output, got:\n{}",
        stdout,
    );
}
