use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

/// Test 1: Create FSDB database in native Delta Lake mode
/// This is the foundational test - FSDB should write directly to Delta Lake format
#[tokio::test]
async fn test_create_fsdb_with_delta_native_mode() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delta_native");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create FSDB database in Delta Lake native mode
    let db = DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone())
        .await
        .unwrap();

    // Insert a batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Verify Delta Lake directory structure exists
    assert!(
        db_path.join("_delta_log").exists(),
        "Delta Lake log directory should exist"
    );

    // Verify at least one transaction log file exists
    let delta_log_dir = std::fs::read_dir(db_path.join("_delta_log")).unwrap();
    let log_files: Vec<_> = delta_log_dir.collect();
    assert!(
        !log_files.is_empty(),
        "Delta Lake transaction log should have at least one file"
    );

    // Query via FSDB API should work
    let results = db.query("SELECT * FROM data").await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 rows");
}

/// Test 2: Open existing Delta Lake table with FSDB
/// FSDB should be able to open and work with tables created by other Delta Lake writers
#[tokio::test]
async fn test_open_existing_delta_table_with_fsdb() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_open_delta");

    // First create using delta-rs directly
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Hello", "World", "Delta"])),
        ],
    )
    .unwrap();

    // Write using delta-rs
    use deltalake::DeltaOps;
    use url::Url;

    // Create directory first
    std::fs::create_dir_all(&db_path).unwrap();

    let table_url = Url::from_directory_path(&db_path).unwrap();
    let ops = DeltaOps::try_from_uri(table_url).await.unwrap();

    let schema_fields = vec![
        deltalake::kernel::StructField::new("id", deltalake::kernel::DataType::INTEGER, false),
        deltalake::kernel::StructField::new("message", deltalake::kernel::DataType::STRING, false),
    ];
    let delta_schema = deltalake::kernel::StructType::try_new(schema_fields).unwrap();

    let table = ops
        .create()
        .with_columns(delta_schema.fields().cloned())
        .await
        .unwrap();

    DeltaOps(table).write(vec![batch]).await.unwrap();

    // Now open with FSDB in Delta native mode
    let db = DatabaseOps::open_delta_native(db_path.clone())
        .await
        .unwrap();

    // Query via FSDB API
    let results = db.query("SELECT * FROM data WHERE id > 1").await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have 2 rows (World, Delta)");

    // Insert more data via FSDB
    let new_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["Lake", "FSDB"])),
        ],
    )
    .unwrap();
    db.insert(new_batch).await.unwrap();

    // Verify total count
    let all_results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = all_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "Should have 5 total rows after FSDB insert");
}

/// Test 3: Transaction commit in Delta Lake native mode
/// Each FSDB transaction should map to exactly one Delta Lake transaction
#[tokio::test]
async fn test_delta_native_transaction_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delta_txn_commit");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Begin transaction and insert multiple batches
    let txn = db.begin_transaction().await.unwrap();

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();
    txn.insert(batch1).await.unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![200])),
        ],
    )
    .unwrap();
    txn.insert(batch2).await.unwrap();

    // Commit
    txn.commit().await.unwrap();

    // Verify data is present
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Should have 2 rows");

    // Verify Delta Lake transaction log structure
    let delta_log_files: Vec<_> = std::fs::read_dir(db_path.join("_delta_log"))
        .unwrap()
        .collect();
    assert!(
        !delta_log_files.is_empty(),
        "Should have Delta Lake transaction log files"
    );
}
