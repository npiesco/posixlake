//! Integration tests for CSV import with auto schema inference
//!
//! Tests that DatabaseOps::create_from_csv():
//! 1. Infers schema from first 10 data rows
//! 2. Detects Int64, Float64, Boolean, and String types
//! 3. Falls back to String when type detection fails
//! 4. Creates valid Delta Lake table
//! 5. Imports all data correctly

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use posixlake::database_ops::DatabaseOps;
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

/// Test CSV import with auto schema inference for mixed types
#[tokio::test]
async fn test_create_from_csv_infers_schema_correctly() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("test.csv");

    // Write CSV with different data types
    // id: Int64, name: String, age: Int64, salary: Float64, active: Boolean
    let mut csv_file = File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,name,age,salary,active").unwrap();
    writeln!(csv_file, "1,Alice,25,50000.50,true").unwrap();
    writeln!(csv_file, "2,Bob,30,60000.75,false").unwrap();
    writeln!(csv_file, "3,Charlie,35,70000.00,true").unwrap();
    writeln!(csv_file, "4,Diana,28,55000.25,true").unwrap();
    writeln!(csv_file, "5,Eve,32,65000.00,false").unwrap();
    writeln!(csv_file, "6,Frank,40,80000.50,true").unwrap();
    writeln!(csv_file, "7,Grace,27,52000.00,false").unwrap();
    writeln!(csv_file, "8,Henry,45,90000.00,true").unwrap();
    writeln!(csv_file, "9,Ivy,29,58000.75,true").unwrap();
    writeln!(csv_file, "10,Jack,33,62000.25,false").unwrap();
    writeln!(csv_file, "11,Kate,38,75000.00,true").unwrap();
    csv_file.flush().unwrap();

    // Create database from CSV
    let db_path = temp_dir.path().join("test_db");
    let db = DatabaseOps::create_from_csv(&db_path, &csv_path)
        .await
        .expect("create_from_csv should succeed");

    // Verify schema was inferred correctly
    let schema = db.schema();
    assert_eq!(schema.fields().len(), 5, "Should have 5 columns");

    // Check column types
    let id_field = schema.field_with_name("id").unwrap();
    let name_field = schema.field_with_name("name").unwrap();
    let age_field = schema.field_with_name("age").unwrap();
    let salary_field = schema.field_with_name("salary").unwrap();
    let active_field = schema.field_with_name("active").unwrap();

    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "id should be Int64"
    );
    assert_eq!(
        name_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "name should be String"
    );
    assert_eq!(
        age_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "age should be Int64"
    );
    assert_eq!(
        salary_field.data_type(),
        &arrow::datatypes::DataType::Float64,
        "salary should be Float64"
    );
    assert_eq!(
        active_field.data_type(),
        &arrow::datatypes::DataType::Boolean,
        "active should be Boolean"
    );

    // Verify all data was imported
    let results = db.query("SELECT COUNT(*) as cnt FROM data").await.unwrap();
    assert_eq!(results.len(), 1);
    let cnt = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 11, "Should have imported 11 rows");

    // Verify data integrity - query for id=1
    let results = db
        .query("SELECT id, name, age, active FROM data WHERE id = 1")
        .await
        .unwrap();
    assert!(!results.is_empty());
    let batch = &results[0];

    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let active = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0);

    assert_eq!(id, 1);
    assert_eq!(name, "Alice");
    assert_eq!(age, 25);
    assert!(active);
}

/// Test that columns with mixed types fall back to String
#[tokio::test]
async fn test_create_from_csv_falls_back_to_string_for_mixed_types() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("mixed.csv");

    // Write CSV where "value" column has mixed types (some numbers, some strings)
    let mut csv_file = File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,value").unwrap();
    writeln!(csv_file, "1,100").unwrap();
    writeln!(csv_file, "2,200").unwrap();
    writeln!(csv_file, "3,not_a_number").unwrap(); // This breaks Int64 inference
    writeln!(csv_file, "4,400").unwrap();
    writeln!(csv_file, "5,500").unwrap();
    writeln!(csv_file, "6,600").unwrap();
    writeln!(csv_file, "7,700").unwrap();
    writeln!(csv_file, "8,800").unwrap();
    writeln!(csv_file, "9,900").unwrap();
    writeln!(csv_file, "10,1000").unwrap();
    csv_file.flush().unwrap();

    let db_path = temp_dir.path().join("mixed_db");
    let db = DatabaseOps::create_from_csv(&db_path, &csv_path)
        .await
        .expect("create_from_csv should succeed");

    // value column should be String because of mixed types
    let schema = db.schema();
    let value_field = schema.field_with_name("value").unwrap();
    assert_eq!(
        value_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "value should fall back to String due to mixed types"
    );

    // Verify data was still imported
    let results = db
        .query("SELECT value FROM data WHERE id = 3")
        .await
        .unwrap();
    assert!(!results.is_empty());
    let value = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value, "not_a_number");
}

/// Test CSV import creates valid Delta Lake table readable by other tools
#[tokio::test]
async fn test_create_from_csv_creates_valid_delta_lake() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("delta.csv");

    let mut csv_file = File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,name").unwrap();
    writeln!(csv_file, "1,Alice").unwrap();
    writeln!(csv_file, "2,Bob").unwrap();
    csv_file.flush().unwrap();

    let db_path = temp_dir.path().join("delta_db");
    let _db = DatabaseOps::create_from_csv(&db_path, &csv_path)
        .await
        .expect("create_from_csv should succeed");

    // Verify Delta Lake structure exists
    let delta_log = db_path.join("_delta_log");
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

    // Verify Parquet files exist
    let parquet_files: Vec<_> = std::fs::read_dir(&db_path)
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
    let reopened = DatabaseOps::open(&db_path)
        .await
        .expect("Should be able to open as Delta Lake table");
    let results = reopened
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
    assert_eq!(cnt, 2);
}

/// Test CSV with empty values creates nullable columns
#[tokio::test]
async fn test_create_from_csv_handles_empty_values() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("nullable.csv");

    let mut csv_file = File::create(&csv_path).unwrap();
    writeln!(csv_file, "id,name,age").unwrap();
    writeln!(csv_file, "1,Alice,25").unwrap();
    writeln!(csv_file, "2,,30").unwrap(); // empty name
    writeln!(csv_file, "3,Charlie,").unwrap(); // empty age
    writeln!(csv_file, "4,Diana,28").unwrap();
    writeln!(csv_file, "5,Eve,32").unwrap();
    writeln!(csv_file, "6,Frank,40").unwrap();
    writeln!(csv_file, "7,Grace,27").unwrap();
    writeln!(csv_file, "8,Henry,45").unwrap();
    writeln!(csv_file, "9,Ivy,29").unwrap();
    writeln!(csv_file, "10,Jack,33").unwrap();
    csv_file.flush().unwrap();

    let db_path = temp_dir.path().join("nullable_db");
    let db = DatabaseOps::create_from_csv(&db_path, &csv_path)
        .await
        .expect("create_from_csv should succeed");

    // All columns should be nullable
    let schema = db.schema();
    for field in schema.fields() {
        assert!(field.is_nullable(), "All columns should be nullable");
    }

    // Verify null values were imported
    let results = db
        .query("SELECT name FROM data WHERE id = 2")
        .await
        .unwrap();
    assert!(!results.is_empty());
    let name_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Name should be null or empty string for id=2
    assert!(
        name_col.is_null(0) || name_col.value(0).is_empty(),
        "name should be null or empty for id=2"
    );
}
