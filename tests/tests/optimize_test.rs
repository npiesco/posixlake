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

/// Test OPTIMIZE operation: compact small Parquet files into larger ones
///
/// This test demonstrates Delta Lake's OPTIMIZE behavior:
/// 1. Creates a database
/// 2. Inserts many small batches (creates many small Parquet files)
/// 3. Counts the Parquet files before optimization
/// 4. Runs OPTIMIZE to compact files
/// 5. Verifies a new compacted file is created (old files remain for time travel)
/// 6. Verifies all data is still intact and queryable
///
/// NOTE: Delta Lake OPTIMIZE creates new compacted files but does NOT physically
/// delete old files - they remain on disk for time travel queries. The VACUUM
/// operation is required to physically remove old files.
#[tokio::test]
async fn test_optimize_compaction() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_optimize";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake OPTIMIZE Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database at: {}", db_path);

    // Insert 20 small batches (each with just 5 rows) to create many small files
    info!("  Inserting 20 small batches (5 rows each)...");
    for i in 0..20 {
        let ids = vec![i * 5, i * 5 + 1, i * 5 + 2, i * 5 + 3, i * 5 + 4];
        let values = vec![
            format!("value_{}", i * 5),
            format!("value_{}", i * 5 + 1),
            format!("value_{}", i * 5 + 2),
            format!("value_{}", i * 5 + 3),
            format!("value_{}", i * 5 + 4),
        ];

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
        )?;

        db.insert(batch).await?;
    }
    info!("  Total rows inserted: 100 (20 batches × 5 rows)");

    // Count Parquet files before optimization
    let files_before = count_parquet_files(db_path)?;
    info!("  Parquet files BEFORE optimize: {}", files_before);
    assert!(
        files_before >= 10,
        "Should have created many small files (found {})",
        files_before
    );

    // Get total size before
    let size_before = get_total_parquet_size(db_path)?;
    info!("  Total size BEFORE optimize: {} bytes", size_before);

    // Verify data integrity before optimization
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count_before = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_before, 100,
        "Should have 100 rows before optimization"
    );
    info!("  Data integrity verified: 100 rows");

    // Run OPTIMIZE
    info!("  Running OPTIMIZE operation...");
    db.optimize().await?;
    info!("  OPTIMIZE completed");

    // Count Parquet files after optimization
    // NOTE: Delta Lake OPTIMIZE does not physically delete old files - they remain for time travel
    // VACUUM is required to physically remove old Parquet files
    let files_after = count_parquet_files(db_path)?;
    info!(
        "  Parquet files AFTER optimize: {} (includes old files for time travel)",
        files_after
    );

    // Verify compaction occurred by checking that we have MORE files now
    // (new compacted file + all the old files that are marked as removed in the log)
    assert!(
        files_after > files_before,
        "After OPTIMIZE, old files remain on disk for time travel: {} before -> {} after",
        files_before,
        files_after
    );

    // The new file count should be old files + 1 new compacted file
    assert_eq!(
        files_after,
        files_before + 1,
        "Should have {} old files + 1 new compacted file = {} total",
        files_before,
        files_before + 1
    );

    // Get total size after
    let size_after = get_total_parquet_size(db_path)?;
    info!("  Total size AFTER optimize: {} bytes", size_after);

    // Size might be similar or slightly different due to recompression
    // But should be in the same ballpark (not drastically different)
    let size_ratio = size_after as f64 / size_before as f64;
    info!("  Size ratio (after/before): {:.2}", size_ratio);
    assert!(
        size_ratio > 0.5 && size_ratio < 2.0,
        "Size should be similar (ratio: {:.2}), not drastically different",
        size_ratio
    );

    // Verify data integrity after optimization (all data still there)
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count_after = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_after, 100,
        "Should still have 100 rows after optimization"
    );

    // Verify specific data is intact
    let results = db
        .query("SELECT id, value FROM data WHERE id = 0 OR id = 50 OR id = 99 ORDER BY id")
        .await?;
    assert_eq!(results[0].num_rows(), 3, "Should find 3 specific rows");

    let ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.value(0), 0);
    assert_eq!(values.value(0), "value_0");
    assert_eq!(ids.value(1), 50);
    assert_eq!(values.value(1), "value_50");
    assert_eq!(ids.value(2), 99);
    assert_eq!(values.value(2), "value_99");

    info!("  Data integrity verified after optimization: all 100 rows present");
    info!(
        "  Physical files: {} -> {} (new compacted file added, old files kept for time travel)",
        files_before, files_after
    );
    info!(
        "  Logical active files in Delta Lake: 1 (old {} files marked as removed in log)",
        files_before
    );

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test OPTIMIZE with filter predicate to compact only matching partitions
/// NOTE: Currently filter-based OPTIMIZE requires partitioned tables
/// This test verifies the error handling for non-partitioned tables
#[tokio::test]
async fn test_optimize_with_filter() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_optimize_filter";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake OPTIMIZE with Filter Test ===");

    // Create schema with partition column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert data for category "A"
    for i in 0..10 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i * 10])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }

    // Insert data for category "B"
    for i in 10..20 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec!["B"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i * 10])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }

    info!("  Inserted 20 small batches (10 for each category)");

    let files_before = count_parquet_files(db_path)?;
    info!("  Parquet files before: {}", files_before);

    // Try to optimize with filter - should return error for non-partitioned table
    info!("  Attempting OPTIMIZE with filter on non-partitioned table...");
    let result = db.optimize_with_filter("category = 'A'").await;

    // Verify we get the expected error
    assert!(result.is_err(), "Should fail for non-partitioned table");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("partitioned") || err_msg.contains("filter"),
        "Error should mention partitioned tables: {}",
        err_msg
    );

    info!("  Correctly rejected filter on non-partitioned table");

    // Run basic optimize without filter (should work)
    info!("  Running basic OPTIMIZE without filter...");
    db.optimize().await?;

    let files_after = count_parquet_files(db_path)?;
    info!("  Parquet files after basic optimize: {}", files_after);

    // Verify all data is still intact
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 20, "All rows should be present");

    info!("  Data integrity verified: all 20 rows present");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test OPTIMIZE with target file size
#[tokio::test]
async fn test_optimize_with_target_size() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_optimize_size";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake OPTIMIZE with Target Size Test ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await?;

    // Insert larger batches
    for i in 0..50 {
        let ids: Vec<i32> = (i * 100..(i + 1) * 100).collect();
        let data: Vec<String> = ids.iter().map(|id| format!("data_{}", id)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(data)) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }

    info!("  Inserted 5000 rows in 50 batches");

    let files_before = count_parquet_files(db_path)?;
    let size_before = get_total_parquet_size(db_path)?;
    info!("  Before: {} files, {} bytes", files_before, size_before);

    // Optimize with target file size (1MB)
    info!("  Running OPTIMIZE with target size: 1MB");
    db.optimize_with_target_size(1024 * 1024).await?;

    let files_after = count_parquet_files(db_path)?;
    let size_after = get_total_parquet_size(db_path)?;
    info!(
        "  After: {} files, {} bytes (includes old files for time travel)",
        files_after, size_after
    );

    // Verify compaction occurred
    // Delta Lake keeps old files, so count should increase by 1 (new compacted file)
    assert_eq!(
        files_after,
        files_before + 1,
        "Should have {} old files + 1 new compacted file",
        files_before
    );

    // Verify data integrity
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5000, "All 5000 rows should be present");

    info!("  Data integrity verified");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

// Helper functions

fn count_parquet_files(db_path: &str) -> Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            count += 1;
        }
    }
    Ok(count)
}

fn get_total_parquet_size(db_path: &str) -> Result<u64> {
    let mut total_size = 0;
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            total_size += entry.metadata()?.len();
        }
    }
    Ok(total_size)
}
