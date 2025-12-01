use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::fs;
use std::sync::Arc;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_dir_all(path);
}

/// Test: Full database backup and restore
#[tokio::test]
async fn test_full_backup_and_restore() {
    setup_logging();
    let db_path = "/tmp/test_db_backup_full";
    let backup_path = "/tmp/test_backup_full";
    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);

    println!("\n=== Test: Full Backup and Restore ===");

    // Create database and insert data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Insert 3 batches of data
    for i in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1, i * 10 + 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    format!("user_{}", i * 10),
                    format!("user_{}", i * 10 + 1),
                    format!("user_{}", i * 10 + 2),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![20 + i, 21 + i, 22 + i])) as ArrayRef,
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
    }

    let results_before = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_before = results_before[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    println!("[OK] Original database has {} rows", count_before);
    assert_eq!(count_before, 9, "Should have 9 rows before backup");

    // Create backup
    db.backup(backup_path).await.expect("Backup should succeed");
    println!("[OK] Backup created at: {}", backup_path);

    // Verify backup files exist (Delta Lake native format)
    assert!(
        fs::metadata(backup_path).unwrap().is_dir(),
        "Backup directory should exist"
    );
    assert!(
        fs::metadata(format!("{}/_delta_log", backup_path))
            .unwrap()
            .is_dir(),
        "Delta Lake transaction log should be backed up"
    );
    // Note: In Delta Lake native mode, _metadata directory is optional (only if it existed in source)
    // Check for parquet files in root
    let has_parquet = fs::read_dir(backup_path)
        .unwrap()
        .any(|entry| entry.unwrap().path().extension().and_then(|s| s.to_str()) == Some("parquet"));
    assert!(has_parquet, "Parquet files should be backed up");
    println!("[OK] Backup structure verified");

    // Restore to a new location
    let restore_path = "/tmp/test_db_restore_full";
    cleanup_test_db(restore_path);

    DatabaseOps::restore(backup_path, restore_path)
        .await
        .expect("Restore should succeed");
    println!("[OK] Database restored to: {}", restore_path);

    // Open restored database and verify data
    let restored_db = DatabaseOps::open(restore_path)
        .await
        .expect("Should open restored database");
    let results_after = restored_db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_after = results_after[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    println!("[OK] Restored database has {} rows", count_after);
    assert_eq!(
        count_after, 9,
        "Restored database should have same row count"
    );

    // Verify actual data integrity
    let original_data = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    let restored_data = restored_db
        .query("SELECT * FROM data ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        original_data.len(),
        restored_data.len(),
        "Should have same number of batches"
    );
    for (orig, rest) in original_data.iter().zip(restored_data.iter()) {
        assert_eq!(
            orig.num_rows(),
            rest.num_rows(),
            "Batches should have same row count"
        );
        assert_eq!(
            orig.num_columns(),
            rest.num_columns(),
            "Batches should have same column count"
        );
    }
    println!("[OK] Data integrity verified");

    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
    cleanup_test_db(restore_path);
}

/// Test: Incremental backup captures only new data since last backup
#[tokio::test]
async fn test_incremental_backup() {
    setup_logging();
    let db_path = "/tmp/test_db_backup_incremental";
    let backup1_path = "/tmp/test_backup_incr1";
    let backup2_path = "/tmp/test_backup_incr2";
    cleanup_test_db(db_path);
    cleanup_test_db(backup1_path);
    cleanup_test_db(backup2_path);

    println!("\n=== Test: Incremental Backup ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100, 200, 300])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch1).await.unwrap();
    println!("[OK] Inserted 3 rows");

    // First full backup
    db.backup(backup1_path).await.unwrap();
    println!("[OK] First backup created");

    // Insert more data
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
            Arc::new(Int32Array::from(vec![400, 500, 600])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch2).await.unwrap();
    println!("[OK] Inserted 3 more rows");

    // Second incremental backup
    db.backup_incremental(backup1_path, backup2_path)
        .await
        .expect("Incremental backup should succeed");
    println!("[OK] Incremental backup created");

    // Verify incremental backup is smaller than full backup
    let backup1_size = get_directory_size(backup1_path);
    let backup2_size = get_directory_size(backup2_path);
    println!("  Full backup size: {} bytes", backup1_size);
    println!("  Incremental backup size: {} bytes", backup2_size);
    assert!(
        backup2_size < backup1_size,
        "Incremental backup should be smaller than full backup"
    );

    cleanup_test_db(db_path);
    cleanup_test_db(backup1_path);
    cleanup_test_db(backup2_path);
}

/// Test: Backup verification ensures integrity
#[tokio::test]
async fn test_backup_verification() {
    setup_logging();
    let db_path = "/tmp/test_db_backup_verify";
    let backup_path = "/tmp/test_backup_verify";
    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);

    println!("\n=== Test: Backup Verification ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Create backup
    db.backup(backup_path).await.unwrap();
    println!("[OK] Backup created");

    // Verify backup
    let verification_result = DatabaseOps::verify_backup(backup_path).await;
    assert!(
        verification_result.is_ok(),
        "Backup verification should succeed"
    );

    let report = verification_result.unwrap();
    println!("[OK] Backup verification passed");
    println!("  Files verified: {}", report.files_verified);
    println!("  Total size: {} bytes", report.total_size_bytes);
    assert!(
        report.files_verified > 0,
        "Should have verified at least one file"
    );
    assert!(
        report.total_size_bytes > 0,
        "Total size should be greater than zero"
    );

    // Corrupt backup and verify it fails (remove Delta Lake transaction log)
    fs::remove_dir_all(format!("{}/_delta_log", backup_path)).ok();
    let corrupt_result = DatabaseOps::verify_backup(backup_path).await;
    assert!(
        corrupt_result.is_err(),
        "Verification should fail for corrupted backup"
    );
    println!("[OK] Corrupted backup detected");

    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
}

/// Test: Point-in-time restore from transaction log
#[tokio::test]
async fn test_point_in_time_restore() {
    setup_logging();
    let db_path = "/tmp/test_db_pitr";
    let backup_path = "/tmp/test_backup_pitr";
    let restore_path = "/tmp/test_db_pitr_restore";
    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
    cleanup_test_db(restore_path);

    println!("\n=== Test: Point-in-Time Restore ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Insert data at different points in time
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn1 = db.insert(batch1).await.unwrap();
    println!("[OK] Transaction 1 committed (txn_id: {})", txn1);

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![200])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn2 = db.insert(batch2).await.unwrap();
    println!("[OK] Transaction 2 committed (txn_id: {})", txn2);

    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![300])) as ArrayRef,
        ],
    )
    .unwrap();
    let _txn3 = db.insert(batch3).await.unwrap();
    println!("[OK] Transaction 3 committed");

    // Backup full database
    db.backup(backup_path).await.unwrap();

    // Restore to a point before transaction 3
    DatabaseOps::restore_to_transaction(backup_path, restore_path, txn2)
        .await
        .expect("Point-in-time restore should succeed");
    println!("[OK] Restored to transaction {}", txn2);

    // Verify restored database has only transactions 1 and 2
    let restored_db = DatabaseOps::open(restore_path)
        .await
        .expect("Should open restored database");
    let results = restored_db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    println!("[OK] Restored database has {} rows (should be 2)", count);
    assert_eq!(count, 2, "Should have restored only first 2 transactions");

    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
    cleanup_test_db(restore_path);
}

/// Test: Backup metadata includes timestamp and row count
#[tokio::test]
async fn test_backup_metadata() {
    setup_logging();
    let db_path = "/tmp/test_db_backup_meta";
    let backup_path = "/tmp/test_backup_meta";
    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);

    println!("\n=== Test: Backup Metadata ===");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Create backup
    db.backup(backup_path).await.unwrap();

    // Read backup metadata
    let metadata = DatabaseOps::get_backup_metadata(backup_path)
        .await
        .expect("Should read backup metadata");

    println!("[OK] Backup metadata:");
    println!("  Timestamp: {}", metadata.timestamp);
    println!("  Total rows: {}", metadata.total_rows);
    println!("  Total files: {}", metadata.total_files);
    println!("  Total size: {} bytes", metadata.total_size_bytes);

    assert!(metadata.timestamp > 0, "Timestamp should be set");
    assert_eq!(metadata.total_rows, 5, "Should have 5 rows in backup");
    assert!(metadata.total_files > 0, "Should have files in backup");
    assert!(
        metadata.total_size_bytes > 0,
        "Backup size should be greater than zero"
    );

    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
}

/// Test: Point-in-time restore across multiple schema versions (v1 -> v2 -> v3 -> v4)
#[tokio::test]
async fn test_point_in_time_restore_multiple_schema_versions() {
    setup_logging();
    let db_path = "/tmp/test_db_pitr_multi_schema";
    let backup_path = "/tmp/test_backup_pitr_multi";
    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);

    println!("\n=== Test: Point-in-Time Restore with Multiple Schema Versions ===");

    // Schema v1: (id, name)
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema_v1.clone())
        .await
        .expect("Failed to create database");

    // Transaction 1-2: Schema v1 data
    let batch1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn1 = db.insert(batch1).await.unwrap();
    println!("[OK] Txn 1: 2 rows, schema v1 (id, name)");

    let batch2 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Charlie"])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn2 = db.insert(batch2).await.unwrap();
    println!("[OK] Txn 2: 1 row, schema v1 (id, name) - Total: 3 rows");

    // Schema v2: (id, name, age)
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let batch3 = RecordBatch::try_new(
        schema_v2.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Dave", "Eve"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![40, 50])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn3 = db.insert(batch3).await.unwrap();
    println!("[OK] Txn 3: 2 rows, schema v2 (id, name, age) - Total: 5 rows");

    // Schema v3: (id, name, age, dept)
    let schema_v3 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("dept", DataType::Utf8, false),
    ]));

    let batch4 = RecordBatch::try_new(
        schema_v3.clone(),
        vec![
            Arc::new(Int32Array::from(vec![6, 7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Frank", "Grace"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![60, 70])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Engineering", "Sales"])) as ArrayRef,
        ],
    )
    .unwrap();
    let txn4 = db.insert(batch4).await.unwrap();
    println!("[OK] Txn 4: 2 rows, schema v3 (id, name, age, dept) - Total: 7 rows");

    // Schema v4: (id, name, age, dept, salary)
    let schema_v4 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]));

    let batch5 = RecordBatch::try_new(
        schema_v4.clone(),
        vec![
            Arc::new(Int32Array::from(vec![8, 9, 10])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Hank", "Ivy", "Jack"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![80, 90, 100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Marketing", "HR", "Engineering"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![80000, 90000, 100000])) as ArrayRef,
        ],
    )
    .unwrap();
    let _txn5 = db.insert(batch5).await.unwrap();
    println!("[OK] Txn 5: 3 rows, schema v4 (id, name, age, dept, salary) - Total: 10 rows");

    // Backup full database
    db.backup(backup_path).await.unwrap();
    println!("\n=== Testing Restores to Different Points ===");

    // Test 1: Restore to txn1 (schema v1, 2 rows)
    let restore1 = "/tmp/test_restore_to_txn1";
    cleanup_test_db(restore1);
    DatabaseOps::restore_to_transaction(backup_path, restore1, txn1)
        .await
        .unwrap();
    let db1 = DatabaseOps::open(restore1).await.unwrap();
    let count1 = db1.query("SELECT COUNT(*) FROM data").await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count1, 2, "Txn1: should have 2 rows");
    assert_eq!(db1.schema().fields().len(), 2, "Txn1: schema v1 (2 fields)");
    println!("[OK] Restore to txn1: 2 rows, 2 fields (id, name)");

    // Test 2: Restore to txn2 (schema v1, 3 rows)
    let restore2 = "/tmp/test_restore_to_txn2";
    cleanup_test_db(restore2);
    DatabaseOps::restore_to_transaction(backup_path, restore2, txn2)
        .await
        .unwrap();
    let db2 = DatabaseOps::open(restore2).await.unwrap();
    let count2 = db2.query("SELECT COUNT(*) FROM data").await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count2, 3, "Txn2: should have 3 rows");
    assert_eq!(db2.schema().fields().len(), 2, "Txn2: schema v1 (2 fields)");
    println!("[OK] Restore to txn2: 3 rows, 2 fields (id, name)");

    // Test 3: Restore to txn3 (schema v2, 5 rows)
    let restore3 = "/tmp/test_restore_to_txn3";
    cleanup_test_db(restore3);
    DatabaseOps::restore_to_transaction(backup_path, restore3, txn3)
        .await
        .unwrap();
    let db3 = DatabaseOps::open(restore3).await.unwrap();
    let count3 = db3.query("SELECT COUNT(*) FROM data").await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count3, 5, "Txn3: should have 5 rows");
    assert_eq!(db3.schema().fields().len(), 3, "Txn3: schema v2 (3 fields)");
    println!("[OK] Restore to txn3: 5 rows, 3 fields (id, name, age)");

    // Test 4: Restore to txn4 (schema v3, 7 rows)
    let restore4 = "/tmp/test_restore_to_txn4";
    cleanup_test_db(restore4);
    DatabaseOps::restore_to_transaction(backup_path, restore4, txn4)
        .await
        .unwrap();
    let db4 = DatabaseOps::open(restore4).await.unwrap();
    let count4 = db4.query("SELECT COUNT(*) FROM data").await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count4, 7, "Txn4: should have 7 rows");
    assert_eq!(db4.schema().fields().len(), 4, "Txn4: schema v3 (4 fields)");
    println!("[OK] Restore to txn4: 7 rows, 4 fields (id, name, age, dept)");

    println!("\n[SUCCESS] All 4 restore points verified with correct schema evolution!");

    cleanup_test_db(db_path);
    cleanup_test_db(backup_path);
    cleanup_test_db(restore1);
    cleanup_test_db(restore2);
    cleanup_test_db(restore3);
    cleanup_test_db(restore4);
}

/// Helper: Calculate total size of a directory
fn get_directory_size(path: &str) -> u64 {
    fn dir_size(path: &std::path::Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = fs::metadata(&path) {
                        total += metadata.len();
                    }
                } else if path.is_dir() {
                    total += dir_size(&path);
                }
            }
        }
        total
    }
    dir_size(std::path::Path::new(path))
}
