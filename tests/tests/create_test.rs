//! Integration tests for CLI create command
//!
//! Tests the `posixlake create` command with various options:
//! - --schema for explicit schema definition
//! - --from-csv for CSV import
//! - --from-parquet for Parquet import

use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::TempDir;

fn posixlake_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // Remove test binary name
    path.pop(); // Remove deps
    path.push("posixlake");
    path
}

/// Test: posixlake create <DB_PATH> --schema "id:Int64,name:String"
#[test]
fn test_cli_create_with_schema() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&db_path)
        .arg("--schema")
        .arg("id:Int64,name:String,active:Boolean")
        .output()
        .expect("Failed to execute posixlake create");

    // Should succeed
    assert!(
        output.status.success(),
        "CLI create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Database directory should exist
    assert!(db_path.exists(), "Database path should exist");

    // Delta Lake log should exist
    let delta_log = db_path.join("_delta_log");
    assert!(delta_log.exists(), "Delta log should exist");

    // Should have at least one commit file
    let log_files: Vec<_> = fs::read_dir(&delta_log)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();
    assert!(!log_files.is_empty(), "Should have Delta Lake commit files");
}

/// Test: posixlake create <DB_PATH> --from-csv <CSV_FILE>
#[test]
fn test_cli_create_from_csv() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("csv_db");
    let csv_path = temp_dir.path().join("data.csv");

    // Create test CSV
    let mut csv_file = fs::File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,name,score").unwrap();
    writeln!(csv_file, "1,Alice,95.5").unwrap();
    writeln!(csv_file, "2,Bob,87.3").unwrap();
    writeln!(csv_file, "3,Charlie,92.1").unwrap();

    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&db_path)
        .arg("--from-csv")
        .arg(&csv_path)
        .output()
        .expect("Failed to execute posixlake create --from-csv");

    assert!(
        output.status.success(),
        "CLI create --from-csv failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Database should exist with Delta Lake structure
    assert!(db_path.exists());
    assert!(db_path.join("_delta_log").exists());

    // Should have parquet data files
    let parquet_files: Vec<_> = fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !parquet_files.is_empty(),
        "Should have Parquet data files after CSV import"
    );
}

/// Test: posixlake create <DB_PATH> --from-parquet <PARQUET_FILE>
#[test]
fn test_cli_create_from_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("parquet_db");

    // First create a source database to get a parquet file
    let source_db_path = temp_dir.path().join("source_db");
    let csv_path = temp_dir.path().join("source.csv");

    let mut csv_file = fs::File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,value").unwrap();
    writeln!(csv_file, "1,100").unwrap();
    writeln!(csv_file, "2,200").unwrap();

    // Create source DB from CSV first
    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&source_db_path)
        .arg("--from-csv")
        .arg(&csv_path)
        .output()
        .expect("Failed to create source database");

    assert!(
        output.status.success(),
        "Failed to create source DB: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Find the parquet file in source DB
    let parquet_file = fs::read_dir(&source_db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .expect("Source DB should have parquet file");

    // Now create new DB from that parquet file
    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&db_path)
        .arg("--from-parquet")
        .arg(parquet_file.path())
        .output()
        .expect("Failed to execute posixlake create --from-parquet");

    assert!(
        output.status.success(),
        "CLI create --from-parquet failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Database should exist with Delta Lake structure
    assert!(db_path.exists());
    assert!(db_path.join("_delta_log").exists());
}

/// Test: Error when no schema source provided
#[test]
fn test_cli_create_requires_schema_source() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("no_schema_db");

    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&db_path)
        .output()
        .expect("Failed to execute posixlake create");

    // Should fail - need either --schema, --from-csv, or --from-parquet
    assert!(
        !output.status.success(),
        "CLI create without schema source should fail"
    );
}

/// Test: Error on invalid schema format
#[test]
fn test_cli_create_invalid_schema_format() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("invalid_schema_db");

    let output = Command::new(posixlake_binary())
        .arg("create")
        .arg(&db_path)
        .arg("--schema")
        .arg("invalid schema format")
        .output()
        .expect("Failed to execute posixlake create");

    // Should fail with invalid schema
    assert!(
        !output.status.success(),
        "CLI create with invalid schema should fail"
    );
}
