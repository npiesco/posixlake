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

/// Test Z-ORDER operation: multi-dimensional clustering for query performance
///
/// This test demonstrates Delta Lake's Z-ORDER (OPTIMIZE ZORDER BY) behavior:
/// 1. Creates a database with multiple columns
/// 2. Inserts data across multiple files
/// 3. Runs Z-ORDER on specific columns to co-locate related data
/// 4. Verifies files are reorganized (new clustered files created)
/// 5. Verifies all data is still intact
///
/// Z-ORDER improves query performance for filters on multiple columns by
/// organizing data using a space-filling curve that preserves locality.
#[tokio::test]
async fn test_zorder_basic() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_zorder_basic";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake Z-ORDER Basic Test ===");

    // Create schema with multiple columns for Z-ORDER
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert data across multiple files with different category/region combinations
    let categories = vec!["A", "B", "C"];
    let regions = vec!["US", "EU", "ASIA"];

    let mut id_counter = 0;
    for category in &categories {
        for region in &regions {
            for _ in 0..5 {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![id_counter])) as ArrayRef,
                        Arc::new(StringArray::from(vec![*category])) as ArrayRef,
                        Arc::new(StringArray::from(vec![*region])) as ArrayRef,
                        Arc::new(Int32Array::from(vec![id_counter * 10])) as ArrayRef,
                    ],
                )?;
                db.insert(batch).await?;
                id_counter += 1;
            }
        }
    }

    info!("  Inserted {} rows across multiple files", id_counter);

    let files_before = count_parquet_files(db_path)?;
    info!("  Parquet files before Z-ORDER: {}", files_before);

    // Run Z-ORDER on category and region columns
    info!("  Running Z-ORDER on [category, region]...");
    db.zorder(&["category", "region"]).await?;

    let files_after = count_parquet_files(db_path)?;
    info!("  Parquet files after Z-ORDER: {}", files_after);

    // Z-ORDER creates new clustered files and marks old files as removed (like OPTIMIZE)
    assert!(
        files_after > files_before,
        "Z-ORDER should create new clustered files: {} -> {}",
        files_before,
        files_after
    );

    // Verify all data is still intact
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count, id_counter as i64,
        "All {} rows should be present",
        id_counter
    );

    // Verify we can still query with filters on Z-ORDERED columns
    let results = db
        .query("SELECT COUNT(*) as count FROM data WHERE category = 'A' AND region = 'US'")
        .await?;
    let filtered_count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        filtered_count, 5,
        "Should have 5 rows for category A, region US"
    );

    info!("  Data integrity verified: all {} rows present", id_counter);
    info!("  Z-ORDER successfully clustered data by [category, region]");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test Z-ORDER with single column
#[tokio::test]
async fn test_zorder_single_column() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_zorder_single";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake Z-ORDER Single Column Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("timestamp", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert data with varying timestamps
    for i in 0..20 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i % 5])) as ArrayRef, // Repeating timestamps
                Arc::new(StringArray::from(vec![format!("value_{}", i)])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }
    info!("  Inserted 20 rows");

    let files_before = count_parquet_files(db_path)?;
    info!("  Parquet files before Z-ORDER: {}", files_before);

    // Run Z-ORDER on single column (timestamp)
    info!("  Running Z-ORDER on [timestamp]...");
    db.zorder(&["timestamp"]).await?;

    let files_after = count_parquet_files(db_path)?;
    info!("  Parquet files after Z-ORDER: {}", files_after);

    // Verify data integrity
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 20, "All 20 rows should be present");

    info!("  Data integrity verified: all 20 rows present");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}

/// Test Z-ORDER combined with OPTIMIZE
///
/// Z-ORDER is typically combined with OPTIMIZE for best results:
/// 1. OPTIMIZE compacts small files
/// 2. Z-ORDER reorganizes the compacted files for better locality
#[tokio::test]
async fn test_zorder_after_optimize() -> Result<()> {
    setup_tracing();

    let db_path = "/tmp/fsdb_test_zorder_optimize";
    let _ = std::fs::remove_dir_all(db_path);

    info!("=== Delta Lake OPTIMIZE + Z-ORDER Combined Test ===");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("priority", DataType::Int32, false),
    ]));

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    info!("  Created database");

    // Insert many small files
    for i in 0..30 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![if i % 3 == 0 {
                    "high"
                } else if i % 3 == 1 {
                    "medium"
                } else {
                    "low"
                }])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i % 10])) as ArrayRef,
            ],
        )?;
        db.insert(batch).await?;
    }
    info!("  Inserted 30 rows across many small files");

    let files_after_insert = count_parquet_files(db_path)?;
    info!("  Files after insert: {}", files_after_insert);

    // First OPTIMIZE to compact files
    info!("  Running OPTIMIZE...");
    db.optimize().await?;

    let files_after_optimize = count_parquet_files(db_path)?;
    info!("  Files after OPTIMIZE: {}", files_after_optimize);

    // Then Z-ORDER to cluster by category and priority
    info!("  Running Z-ORDER on [category, priority]...");
    db.zorder(&["category", "priority"]).await?;

    let files_after_zorder = count_parquet_files(db_path)?;
    info!("  Files after Z-ORDER: {}", files_after_zorder);

    // Verify data integrity
    let results = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 30, "All 30 rows should be present");

    // Verify filtered queries work correctly
    let results = db
        .query("SELECT COUNT(*) as count FROM data WHERE category = 'high'")
        .await?;
    let high_count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(high_count, 10, "Should have 10 'high' category rows");

    info!("  Data integrity verified: OPTIMIZE + Z-ORDER successful");

    // Cleanup
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
