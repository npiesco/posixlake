//! Integration tests for Parquet import
//!
//! Tests that DatabaseOps::create_from_parquet():
//! 1. Reads schema directly from Parquet file metadata
//! 2. Creates valid Delta Lake table
//! 3. Imports all data correctly
//! 4. Supports single file and glob patterns

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use posixlake::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

/// Test creating a Delta Lake database from a single Parquet file
#[tokio::test]
async fn test_create_from_parquet_single_file() {
    let temp_dir = TempDir::new().unwrap();

    // First create a source database and insert data to generate Parquet files
    let source_path = temp_dir.path().join("source_db");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, true),
    ]));

    let source_db = DatabaseOps::create(&source_path, schema.clone())
        .await
        .unwrap();

    // Insert data
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec![Some("NYC"), Some("SF"), None])),
        ],
    )
    .unwrap();
    source_db.insert(batch).await.unwrap();

    // Find the Parquet file that was created
    let parquet_files: Vec<_> = std::fs::read_dir(&source_path)
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
        "Should have created Parquet file"
    );

    let parquet_path = parquet_files[0].path();

    // Now create a new database from this Parquet file
    let target_path = temp_dir.path().join("target_db");
    let target_db = DatabaseOps::create_from_parquet(&target_path, &parquet_path)
        .await
        .expect("create_from_parquet should succeed");

    // Verify schema was read correctly
    let target_schema = target_db.schema();
    assert_eq!(target_schema.fields().len(), 3);
    assert_eq!(target_schema.field(0).name(), "id");
    assert_eq!(target_schema.field(1).name(), "name");
    assert_eq!(target_schema.field(2).name(), "city");

    // Verify data was imported
    let results = target_db
        .query("SELECT COUNT(*) as cnt FROM data")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let cnt = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 3, "Should have imported 3 rows");

    // Verify data integrity
    let results = target_db
        .query("SELECT id, name FROM data WHERE id = 1")
        .await
        .unwrap();
    assert!(!results.is_empty());
    let id = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    let name = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(id, 1);
    assert_eq!(name, "Alice");
}

/// Test creating a Delta Lake database from multiple Parquet files via glob
#[tokio::test]
async fn test_create_from_parquet_glob_pattern() {
    let temp_dir = TempDir::new().unwrap();

    // Create source database with multiple inserts to generate multiple Parquet files
    let source_path = temp_dir.path().join("source_db");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let source_db = DatabaseOps::create(&source_path, schema.clone())
        .await
        .unwrap();

    // Insert data in multiple batches to create multiple Parquet files
    for i in 0..3 {
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i * 10 + 1, i * 10 + 2])),
                Arc::new(StringArray::from(vec![
                    format!("value_{}", i * 10 + 1),
                    format!("value_{}", i * 10 + 2),
                ])),
            ],
        )
        .unwrap();
        source_db.insert(batch).await.unwrap();
    }

    // Create target database from all Parquet files using glob
    let target_path = temp_dir.path().join("target_db");
    let glob_pattern = source_path.join("*.parquet");

    let target_db = DatabaseOps::create_from_parquet(&target_path, &glob_pattern)
        .await
        .expect("create_from_parquet with glob should succeed");

    // Verify all data was imported (3 batches x 2 rows = 6 rows)
    let results = target_db
        .query("SELECT COUNT(*) as cnt FROM data")
        .await
        .unwrap();
    let cnt = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 6, "Should have imported 6 rows from 3 Parquet files");
}

/// Test that create_from_parquet creates valid Delta Lake structure
#[tokio::test]
async fn test_create_from_parquet_creates_valid_delta_lake() {
    let temp_dir = TempDir::new().unwrap();

    // Create source Parquet file
    let source_path = temp_dir.path().join("source_db");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let source_db = DatabaseOps::create(&source_path, schema.clone())
        .await
        .unwrap();

    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    source_db.insert(batch).await.unwrap();

    // Get Parquet file path
    let parquet_file = std::fs::read_dir(&source_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .unwrap()
        .path();

    // Create target database from Parquet
    let target_path = temp_dir.path().join("target_db");
    let _target_db = DatabaseOps::create_from_parquet(&target_path, &parquet_file)
        .await
        .expect("create_from_parquet should succeed");

    // Verify Delta Lake structure exists
    let delta_log = target_path.join("_delta_log");
    assert!(delta_log.exists(), "_delta_log directory should exist");

    // Verify at least one transaction log file exists
    let log_files: Vec<_> = std::fs::read_dir(&delta_log)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !log_files.is_empty(),
        "Should have at least one transaction log file"
    );

    // Verify Parquet files exist in target
    let parquet_files: Vec<_> = std::fs::read_dir(&target_path)
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
        "Should have at least one Parquet file"
    );

    // Re-open with DatabaseOps::open to verify it's a valid Delta Lake table
    let reopened = DatabaseOps::open(&target_path)
        .await
        .expect("Should be able to open as Delta Lake table");
    let results = reopened
        .query("SELECT COUNT(*) as cnt FROM data")
        .await
        .unwrap();
    let cnt = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 2);
}
