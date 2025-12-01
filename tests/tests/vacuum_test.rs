use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fsdb::Result;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tracing::info;

fn setup_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
}

fn count_parquet_files(db_path: &str) -> Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
            count += 1;
        }
    }
    Ok(count)
}

/// Test VACUUM operation: physically delete old Parquet files
///
/// This test demonstrates Delta Lake's VACUUM behavior:
/// 1. Creates small files via multiple inserts
/// 2. Runs OPTIMIZE to create new compacted file (old files marked as removed)
/// 3. Verifies old files still exist on disk (for time travel)
/// 4. Runs VACUUM to physically delete old files
/// 5. Verifies old files are now deleted
/// 6. Verifies current data is still intact
///
/// VACUUM removes files older than the retention period that are not referenced
/// by the current Delta Lake snapshot.
#[tokio::test]
async fn test_vacuum_basic() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_vacuum_basic";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake VACUUM Basic Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert 10 small batches (creates 10 small Parquet files)
    for i in 0..10 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("value_{}", i)])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }
    info!("  Inserted 10 small batches");

    let files_after_insert = count_parquet_files(db_path)?;
    info!("  Parquet files after insert: {}", files_after_insert);
    assert_eq!(files_after_insert, 10, "Should have 10 small files");

    // Run OPTIMIZE to compact files
    info!("  Running OPTIMIZE...");
    db.optimize().await?;

    let files_after_optimize = count_parquet_files(db_path)?;
    info!("  Parquet files after OPTIMIZE: {}", files_after_optimize);
    assert_eq!(
        files_after_optimize, 11,
        "Should have 10 old files + 1 new compacted file"
    );

    // Run VACUUM with 0 retention (delete all unreferenced files immediately)
    info!("  Running VACUUM with 0 hour retention...");
    db.vacuum(0).await?;

    let files_after_vacuum = count_parquet_files(db_path)?;
    info!("  Parquet files after VACUUM: {}", files_after_vacuum);
    assert_eq!(
        files_after_vacuum, 1,
        "Should have only 1 compacted file remaining"
    );

    // Verify all data is still intact
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10, "All 10 rows should be present");

    info!("  Data integrity verified: all 10 rows present");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test VACUUM with retention period
///
/// Files newer than the retention period should NOT be deleted,
/// even if they're marked as removed in the transaction log.
#[tokio::test]
async fn test_vacuum_with_retention() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_vacuum_retention";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake VACUUM with Retention Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert 5 small batches
    for i in 0..5 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("value_{}", i)])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }
    info!("  Inserted 5 small batches");

    let files_before = count_parquet_files(db_path)?;
    info!("  Parquet files before OPTIMIZE: {}", files_before);

    // Run OPTIMIZE
    info!("  Running OPTIMIZE...");
    db.optimize().await?;

    let files_after_optimize = count_parquet_files(db_path)?;
    info!("  Parquet files after OPTIMIZE: {}", files_after_optimize);
    assert_eq!(
        files_after_optimize,
        files_before + 1,
        "Should have old files + 1 new compacted file"
    );

    // Run VACUUM with 168 hour (7 day) retention
    // Since files were just created, they should NOT be deleted
    info!("  Running VACUUM with 168 hour (7 day) retention...");
    db.vacuum(168).await?;

    let files_after_vacuum = count_parquet_files(db_path)?;
    info!(
        "  Parquet files after VACUUM with retention: {}",
        files_after_vacuum
    );
    assert_eq!(
        files_after_vacuum, files_after_optimize,
        "No files should be deleted with 7-day retention (all files are new)"
    );

    // Verify data integrity
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "All 5 rows should be present");

    info!("  Data integrity verified: all 5 rows present");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test VACUUM dry run (preview mode)
///
/// Dry run should report what would be deleted without actually deleting.
#[tokio::test]
async fn test_vacuum_dry_run() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_vacuum_dry_run";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake VACUUM Dry Run Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert 10 small batches
    for i in 0..10 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("value_{}", i)])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }
    info!("  Inserted 10 small batches");

    // Run OPTIMIZE
    info!("  Running OPTIMIZE...");
    db.optimize().await?;

    let files_before_vacuum = count_parquet_files(db_path)?;
    info!("  Parquet files before VACUUM: {}", files_before_vacuum);

    // Run VACUUM dry run with 0 retention
    info!("  Running VACUUM dry run...");
    let delete_list = db.vacuum_dry_run(0).await?;

    info!("  Dry run would delete {} files", delete_list.len());
    assert!(
        delete_list.len() >= 10,
        "Dry run should report at least 10 old files for deletion"
    );

    // Verify no files were actually deleted
    let files_after_dry_run = count_parquet_files(db_path)?;
    info!("  Parquet files after dry run: {}", files_after_dry_run);
    assert_eq!(
        files_after_dry_run, files_before_vacuum,
        "Dry run should not delete any files"
    );

    // Now run actual VACUUM
    info!("  Running actual VACUUM...");
    db.vacuum(0).await?;

    let files_after_vacuum = count_parquet_files(db_path)?;
    info!("  Parquet files after real VACUUM: {}", files_after_vacuum);
    assert!(
        files_after_vacuum < files_before_vacuum,
        "Real VACUUM should delete old files"
    );

    // Verify data integrity
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10, "All 10 rows should be present");

    info!("  Data integrity verified: all 10 rows present");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
