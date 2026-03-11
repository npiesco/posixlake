//! Integration tests for CLI health and metrics commands

use std::process::Command;
use std::sync::OnceLock;
use tempfile::TempDir;

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
            .expect("failed to build posixlake-cli");
        assert!(
            output.status.success(),
            "failed to build posixlake-cli:\n{}",
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
            "binary not found at {:?}",
            binary_path
        );
        binary_path
    })
}

fn create_test_db(dir: &std::path::Path) -> std::path::PathBuf {
    let db_path = dir.join("testdb");
    let binary = build_posixlake_binary();
    let output = Command::new(binary)
        .arg("create")
        .arg(&db_path)
        .arg("--schema")
        .arg("id:Int64,name:String")
        .output()
        .expect("failed to run create");
    assert!(
        output.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    db_path
}

/// CLI `health` returns valid JSON with expected fields
#[test]
fn test_cli_health_returns_json() {
    let tmp = TempDir::new().unwrap();
    let db_path = create_test_db(tmp.path());
    let binary = build_posixlake_binary();

    let output = Command::new(binary)
        .arg("health")
        .arg(&db_path)
        .output()
        .expect("failed to run health");

    assert!(
        output.status.success(),
        "health failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("health output not valid JSON: {}\noutput: {}", e, stdout));

    assert_eq!(json["status"], "healthy");
    assert!(json["uptime_seconds"].as_f64().unwrap() >= 0.0);
    assert!(json.get("total_files").is_some());
    assert!(json.get("total_rows").is_some());
    assert!(json.get("total_size_bytes").is_some());
}

/// CLI `metrics` returns valid JSON with expected fields
#[test]
fn test_cli_metrics_returns_json() {
    let tmp = TempDir::new().unwrap();
    let db_path = create_test_db(tmp.path());
    let binary = build_posixlake_binary();

    let output = Command::new(binary)
        .arg("metrics")
        .arg(&db_path)
        .output()
        .expect("failed to run metrics");

    assert!(
        output.status.success(),
        "metrics failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("metrics output not valid JSON: {}\noutput: {}", e, stdout));

    assert_eq!(json["total_queries"], 0);
    assert_eq!(json["total_inserts"], 0);
    assert_eq!(json["total_deletes"], 0);
    assert_eq!(json["total_transactions"], 0);
    assert_eq!(json["total_errors"], 0);
    assert!(json["uptime_seconds"].as_f64().unwrap() >= 0.0);
}

/// CLI `health` with auth requires credentials
#[test]
fn test_cli_health_with_auth() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("authdb");
    let binary = build_posixlake_binary();

    // Create with auth
    let output = Command::new(binary)
        .arg("create")
        .arg(&db_path)
        .arg("--schema")
        .arg("id:Int64,name:String")
        .arg("--auth")
        .arg("--admin-password")
        .arg("testpass123")
        .output()
        .expect("failed to create auth db");
    assert!(
        output.status.success(),
        "create with auth failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Health with valid credentials should succeed
    let output = Command::new(binary)
        .arg("health")
        .arg(&db_path)
        .arg("-u")
        .arg("admin")
        .arg("--password")
        .arg("testpass123")
        .output()
        .expect("failed to run health with auth");
    assert!(
        output.status.success(),
        "health with auth failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "healthy");
}

/// CLI `metrics` on nonexistent DB fails gracefully
#[test]
fn test_cli_metrics_nonexistent_db() {
    let binary = build_posixlake_binary();
    let output = Command::new(binary)
        .arg("metrics")
        .arg("/tmp/posixlake-nonexistent-db-xyz")
        .output()
        .expect("failed to run metrics");

    assert!(
        !output.status.success(),
        "metrics on nonexistent DB should fail"
    );
}
