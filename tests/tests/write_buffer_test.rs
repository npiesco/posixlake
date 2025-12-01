//! Integration tests for WriteBuffer - batching multiple small writes
//!
//! Tests that WriteBuffer batches multiple small CSV appends into fewer Delta Lake transactions
//! for improved performance.

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_write_buffer_batches_multiple_appends() {
    let _ = tracing_subscriber::fmt::try_init();

    // Create test database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Get initial transaction count
    let initial_version = db.get_delta_table().await.unwrap().version().unwrap_or(0);

    // Perform 10 small appends via WriteBuffer
    // Each append is tiny (1 row), but they should be batched
    for i in 1..=10 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("row_{}", i)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i * 10])) as ArrayRef,
            ],
        )
        .unwrap();

        // Insert via WriteBuffer (will batch internally)
        db.insert_buffered(batch).await.unwrap();
    }

    // Force flush to ensure all writes are committed
    db.flush_write_buffer().await.unwrap();

    // Get final transaction count
    let final_version = db.get_delta_table().await.unwrap().version().unwrap_or(0);

    let transactions_created = final_version - initial_version;

    // Verify data was written correctly
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 10, "Should have 10 rows");

    // CRITICAL: Verify batching happened - should use fewer than 10 transactions
    // Without buffering, each insert would create 1 transaction = 10 total
    // With buffering, should batch into ~2-3 transactions max
    println!(
        "Transactions created: {} (10 appends)",
        transactions_created
    );
    assert!(
        transactions_created < 10,
        "WriteBuffer should batch appends into fewer transactions. Got {} transactions for 10 appends",
        transactions_created
    );

    // Ideally should be 1-3 transactions
    assert!(
        transactions_created <= 3,
        "WriteBuffer should batch efficiently. Expected <= 3 transactions, got {}",
        transactions_created
    );
}

#[tokio::test]
async fn test_write_buffer_auto_flush_on_size() {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    let initial_version = db.get_delta_table().await.unwrap().version().unwrap_or(0);

    // Write enough data to trigger auto-flush based on row count (>1000 rows default)
    let large_string = "x".repeat(1024); // 1KB per row

    for i in 0..1100 {
        // This should trigger auto-flush when buffer reaches 1000 rows
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![large_string.clone()])) as ArrayRef,
            ],
        )
        .unwrap();

        db.insert_buffered(batch).await.unwrap();
    }

    // Need manual flush for remaining rows (last 100 rows after auto-flush)
    db.flush_write_buffer().await.unwrap();

    // Now all data should be committed
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(count_array.value(0), 1100, "Should have all 1100 rows");

    let final_version = db.get_delta_table().await.unwrap().version().unwrap_or(0);
    let transactions = final_version - initial_version;

    // Should have auto-flushed at least once, but not 1100 times
    println!(
        "Auto-flush created {} transactions for 1100 rows (~1.1MB)",
        transactions
    );
    assert!(
        transactions > 0,
        "Should have committed at least one transaction"
    );
    assert!(
        transactions < 1100,
        "Should batch writes, not create transaction per row"
    );
}
