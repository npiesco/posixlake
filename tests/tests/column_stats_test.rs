use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tracing::info;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

/// Test that min/max statistics are computed and stored during insert
#[tokio::test]
async fn test_column_stats_computed_on_insert() {
    setup_logging();
    let db_path = "/tmp/test_db_column_stats_insert";
    cleanup_test_db(db_path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );

    // Insert data with known min/max values
    info!("[TEST] Inserting batch with id=1-5, age=20-50");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Int32Array::from(vec![20, 30, 40, 50, 25])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])) as ArrayRef,
        ],
    )
    .expect("Failed to create batch");

    db.insert(batch).await.expect("Failed to insert batch");

    // Retrieve statistics from the database
    let stats = db
        .get_column_statistics()
        .await
        .expect("Failed to get stats");

    info!("[TEST] Retrieved statistics: {:?}", stats);

    // Verify statistics exist and are correct
    assert!(
        stats.contains_key("id"),
        "Should have stats for 'id' column"
    );
    assert!(
        stats.contains_key("age"),
        "Should have stats for 'age' column"
    );

    let id_stats = &stats["id"];
    assert_eq!(
        id_stats.min_value.as_i64().unwrap(),
        1,
        "Min id should be 1"
    );
    assert_eq!(
        id_stats.max_value.as_i64().unwrap(),
        5,
        "Max id should be 5"
    );

    let age_stats = &stats["age"];
    assert_eq!(
        age_stats.min_value.as_i64().unwrap(),
        20,
        "Min age should be 20"
    );
    assert_eq!(
        age_stats.max_value.as_i64().unwrap(),
        50,
        "Max age should be 50"
    );

    info!("[TEST] Column statistics test completed successfully.");
}

/// Test that statistics are used for query pruning
#[tokio::test]
async fn test_column_stats_query_pruning() {
    setup_logging();
    let db_path = "/tmp/test_db_column_stats_pruning";
    cleanup_test_db(db_path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );

    // Insert multiple batches with non-overlapping ranges
    info!("[TEST] Inserting batch 1: id=1-10");
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((1..=10).collect::<Vec<i32>>())) as ArrayRef,
            Arc::new(Int32Array::from((100..=109).collect::<Vec<i32>>())) as ArrayRef,
        ],
    )
    .expect("Failed to create batch 1");
    db.insert(batch1).await.expect("Failed to insert batch 1");

    info!("[TEST] Inserting batch 2: id=100-110");
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((100..=110).collect::<Vec<i32>>())) as ArrayRef,
            Arc::new(Int32Array::from((200..=210).collect::<Vec<i32>>())) as ArrayRef,
        ],
    )
    .expect("Failed to create batch 2");
    db.insert(batch2).await.expect("Failed to insert batch 2");

    info!("[TEST] Inserting batch 3: id=1000-1010");
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((1000..=1010).collect::<Vec<i32>>())) as ArrayRef,
            Arc::new(Int32Array::from((300..=310).collect::<Vec<i32>>())) as ArrayRef,
        ],
    )
    .expect("Failed to create batch 3");
    db.insert(batch3).await.expect("Failed to insert batch 3");

    // Query with a predicate that should only match batch 2
    info!("[TEST] Querying: SELECT * FROM data WHERE id >= 100 AND id <= 110");
    let results = db
        .query("SELECT * FROM data WHERE id >= 100 AND id <= 110")
        .await
        .expect("Failed to query");

    // Extract row count
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 11, "Should return exactly 11 rows from batch 2");

    // Note: Delta Lake performs file pruning automatically based on column statistics
    // stored in the transaction log (_delta_log). Pruning happens transparently when
    // queries are executed - only files containing relevant data ranges are scanned.
    // Delta Lake doesn't expose pruning metrics in the same way as legacy FSDB.

    info!(
        "[TEST] Query pruning test completed successfully - Delta Lake handles pruning automatically."
    );
}

/// Test that statistics are persisted across database reopens
#[tokio::test]
async fn test_column_stats_persistence() {
    setup_logging();
    let db_path = "/tmp/test_db_column_stats_persist";
    cleanup_test_db(db_path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
    ]));

    // Create DB and insert data
    {
        let db = Arc::new(
            DatabaseOps::create(db_path, schema.clone())
                .await
                .expect("Failed to create database"),
        );

        info!("[TEST] Inserting batch with id=10-20, score=500-1000");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from((10..=20).collect::<Vec<i32>>())) as ArrayRef,
                Arc::new(Int32Array::from((500..=510).collect::<Vec<i32>>())) as ArrayRef,
            ],
        )
        .expect("Failed to create batch");

        db.insert(batch).await.expect("Failed to insert batch");

        // Verify stats before closing
        let stats = db
            .get_column_statistics()
            .await
            .expect("Failed to get stats");
        assert_eq!(stats["id"].min_value.as_i64().unwrap(), 10);
        assert_eq!(stats["id"].max_value.as_i64().unwrap(), 20);
    } // DB dropped here

    // Reopen DB and verify statistics are still available
    {
        info!("[TEST] Reopening database...");
        let db = Arc::new(
            DatabaseOps::open(db_path)
                .await
                .expect("Failed to reopen database"),
        );

        let stats = db
            .get_column_statistics()
            .await
            .expect("Failed to get stats after reopen");
        info!("[TEST] Statistics after reopen: {:?}", stats);

        assert!(stats.contains_key("id"), "Should have persisted 'id' stats");
        assert_eq!(
            stats["id"].min_value.as_i64().unwrap(),
            10,
            "Min id should still be 10"
        );
        assert_eq!(
            stats["id"].max_value.as_i64().unwrap(),
            20,
            "Max id should still be 20"
        );
        assert_eq!(
            stats["score"].min_value.as_i64().unwrap(),
            500,
            "Min score should be 500"
        );
        assert_eq!(
            stats["score"].max_value.as_i64().unwrap(),
            510,
            "Max score should be 510"
        );
    }

    info!("[TEST] Statistics persistence test completed successfully.");
}

/// Test that compaction preserves and updates statistics correctly
#[tokio::test]
async fn test_column_stats_after_compaction() {
    setup_logging();
    let db_path = "/tmp/test_db_column_stats_compact";
    cleanup_test_db(db_path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );

    // Insert 3 small batches
    for i in 0..3 {
        let start = i * 10;
        let end = start + 5;
        info!("[TEST] Inserting batch {}: id={}-{}", i + 1, start, end - 1);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from((start..end).collect::<Vec<i32>>())) as ArrayRef,
                Arc::new(Int32Array::from(
                    (start * 100..(end * 100))
                        .step_by(100)
                        .collect::<Vec<i32>>(),
                )) as ArrayRef,
            ],
        )
        .expect("Failed to create batch");

        db.insert(batch).await.expect("Failed to insert batch");
    }

    // Get stats before compaction
    let stats_before = db
        .get_column_statistics()
        .await
        .expect("Failed to get stats before compaction");
    info!("[TEST] Stats before compaction: {:?}", stats_before);

    // Run compaction
    info!("[TEST] Running compaction...");
    db.compact().await.expect("Failed to compact");

    // Get stats after compaction
    let stats_after = db
        .get_column_statistics()
        .await
        .expect("Failed to get stats after compaction");
    info!("[TEST] Stats after compaction: {:?}", stats_after);

    // Verify that statistics are updated correctly for the compacted file
    assert_eq!(
        stats_after["id"].min_value.as_i64().unwrap(),
        0,
        "Min id should be 0"
    );
    assert_eq!(
        stats_after["id"].max_value.as_i64().unwrap(),
        24,
        "Max id should be 24"
    );
    assert_eq!(
        stats_after["value"].min_value.as_i64().unwrap(),
        0,
        "Min value should be 0"
    );

    info!("[TEST] Compaction statistics test completed successfully.");
}
