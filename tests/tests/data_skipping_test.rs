use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

/// Test that data skipping works based on min/max statistics
///
/// This test creates multiple Parquet files with distinct value ranges:
/// - File 1: age 20-30
/// - File 2: age 40-50
/// - File 3: age 60-70
///
/// When querying for age > 55, only File 3 should be read.
#[tokio::test]
async fn test_data_skipping_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    // Create database
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert batch 1: ages 20-30 (with many rows to ensure it stays as separate file)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((1..=100).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Alice"; 100])),
            Arc::new(Int32Array::from(
                (20..=29).cycle().take(100).collect::<Vec<i32>>(),
            )),
        ],
    )
    .unwrap();
    db.insert(batch1).await.unwrap();

    // Insert batch 2: ages 40-50 (separate file)
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((101..=200).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Dave"; 100])),
            Arc::new(Int32Array::from(
                (40..=49).cycle().take(100).collect::<Vec<i32>>(),
            )),
        ],
    )
    .unwrap();
    db.insert(batch2).await.unwrap();

    // Insert batch 3: ages 60-70 (separate file)
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((201..=300).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Grace"; 100])),
            Arc::new(Int32Array::from(
                (60..=69).cycle().take(100).collect::<Vec<i32>>(),
            )),
        ],
    )
    .unwrap();
    db.insert(batch3).await.unwrap();

    // Query with filter that should only match file 3 (age > 55)
    let results = db
        .query("SELECT name, age FROM data WHERE age > 55 ORDER BY age")
        .await
        .unwrap();

    // Should only return data from batch 3 (ages 60-69)
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 100,
        "Should return 100 rows from age 60-69 range"
    );

    // Verify correct age range
    let first_batch = &results[0];
    let ages = first_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(ages.value(0) >= 60, "All ages should be >= 60");

    // Verify that data skipping actually happened by checking file read count
    // The key insight: Delta Lake + DataFusion should skip files based on stats
    let skipping_stats = db.get_data_skipping_stats().await.unwrap();
    println!(
        "Data skipping stats: total_files={}, files_read={}, files_skipped={}",
        skipping_stats.total_files, skipping_stats.files_read, skipping_stats.files_skipped
    );

    // Debug: List parquet files
    let parquet_count = std::fs::read_dir(&db_path)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("parquet")
        })
        .count();
    println!("Parquet files found: {}", parquet_count);

    assert!(
        skipping_stats.total_files >= 3,
        "Should have at least 3 files, got {}",
        skipping_stats.total_files
    );
    assert!(
        skipping_stats.files_skipped >= 2,
        "Should skip at least 2 files (age 20-30 and 40-50 ranges)"
    );
    assert_eq!(
        skipping_stats.files_read, 1,
        "Should only read 1 file (age 60-70 range)"
    );
}

/// Test data skipping with range query
#[tokio::test]
async fn test_data_skipping_with_range() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // File 1: values 0-99
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>())),
            Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>())),
        ],
    )
    .unwrap();
    db.insert(batch1).await.unwrap();

    // File 2: values 200-299
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((100..200).collect::<Vec<i32>>())),
            Arc::new(Int32Array::from((200..300).collect::<Vec<i32>>())),
        ],
    )
    .unwrap();
    db.insert(batch2).await.unwrap();

    // File 3: values 400-499
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((200..300).collect::<Vec<i32>>())),
            Arc::new(Int32Array::from((400..500).collect::<Vec<i32>>())),
        ],
    )
    .unwrap();
    db.insert(batch3).await.unwrap();

    // Query for values 210-290 (should only read file 2)
    let results = db
        .query("SELECT COUNT(*) as count FROM data WHERE value >= 210 AND value <= 290")
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 81); // 290 - 210 + 1

    let skipping_stats = db.get_data_skipping_stats().await.unwrap();
    assert_eq!(
        skipping_stats.files_skipped, 2,
        "Should skip 2 files (ranges 0-100 and 400-500)"
    );
    assert_eq!(
        skipping_stats.files_read, 1,
        "Should only read 1 file (range 200-300)"
    );
}

/// Test data skipping with string predicates
#[tokio::test]
async fn test_data_skipping_with_strings() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // File 1: categories A-C (100 rows to keep separate)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((1..=100).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Apple"; 100])),
        ],
    )
    .unwrap();
    db.insert(batch1).await.unwrap();

    // File 2: categories M-O
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((101..=200).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Mango"; 100])),
        ],
    )
    .unwrap();
    db.insert(batch2).await.unwrap();

    // File 3: categories W-Z
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((201..=300).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(vec!["Watermelon"; 100])),
        ],
    )
    .unwrap();
    db.insert(batch3).await.unwrap();

    // Query for categories starting with 'W' or later (should only read file 3)
    let results = db
        .query("SELECT category FROM data WHERE category >= 'W' ORDER BY category")
        .await
        .unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 100,
        "Should return 100 rows from Watermelon category"
    );

    let skipping_stats = db.get_data_skipping_stats().await.unwrap();
    assert_eq!(skipping_stats.files_skipped, 2, "Should skip 2 files");
    assert_eq!(skipping_stats.files_read, 1, "Should only read 1 file");
}

/// Test that no files are skipped when all are needed
#[tokio::test]
async fn test_no_data_skipping_when_all_needed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Create 3 files with overlapping ranges (100 rows each to ensure separate files)
    for i in 0..3 {
        let start_id = i * 100;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(
                    (start_id..start_id + 100).collect::<Vec<i32>>(),
                )),
                Arc::new(Int32Array::from(
                    (start_id..start_id + 100).collect::<Vec<i32>>(),
                )),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
    }

    // Query with no filter (should read all files)
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 300); // 3 files * 100 rows each

    let skipping_stats = db.get_data_skipping_stats().await.unwrap();
    assert_eq!(skipping_stats.files_skipped, 0, "Should not skip any files");
    assert_eq!(skipping_stats.files_read, 3, "Should read all 3 files");
}
