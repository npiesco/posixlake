use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

/// Test basic explicit transaction: begin -> insert -> commit
#[tokio::test]
async fn test_explicit_transaction_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_explicit_txn");

    // Create schema
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create database
    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Begin explicit transaction
    let txn = db.begin_transaction().await.unwrap();

    // Insert data within transaction
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    txn.insert(batch).await.unwrap();

    // Commit transaction
    txn.commit().await.unwrap();

    // Query to verify data
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);
}

/// Test multiple inserts in single transaction create ONE file
#[tokio::test]
async fn test_transaction_batches_multiple_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_batched_txn");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Begin transaction
    let txn = db.begin_transaction().await.unwrap();

    // Insert 3 batches within same transaction
    for i in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1])),
                Arc::new(StringArray::from(vec![
                    format!("value_{}", i * 2),
                    format!("value_{}", i * 2 + 1),
                ])),
            ],
        )
        .unwrap();
        txn.insert(batch).await.unwrap();
    }

    // Commit once
    txn.commit().await.unwrap();

    // Verify all 6 rows are present
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 6);

    // Note: Delta Lake manages file organization internally
    // Multiple inserts in a transaction are batched into a single commit
}

/// Test rollback functionality
#[tokio::test]
async fn test_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_rollback");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Begin transaction
    let txn = db.begin_transaction().await.unwrap();

    // Insert data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    txn.insert(batch).await.unwrap();

    // Rollback instead of commit
    txn.rollback().await.unwrap();

    // Verify no data was persisted
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 0, "Expected 0 rows after rollback");
}

/// Test query within transaction (read your own writes)
#[tokio::test]
async fn test_transaction_read_your_own_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_read_own_writes");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    let txn = db.begin_transaction().await.unwrap();

    // Insert batch 1
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();
    txn.insert(batch1).await.unwrap();

    // Query within transaction - should see uncommitted data
    let results = txn.query("SELECT * FROM data").await.unwrap();
    assert_eq!(
        results[0].num_rows(),
        1,
        "Should see own uncommitted writes"
    );

    // Insert batch 2
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![200])),
        ],
    )
    .unwrap();
    txn.insert(batch2).await.unwrap();

    // Query again - should see both batches
    let results = txn
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count_array.value(0),
        2,
        "Should see both uncommitted batches"
    );

    // Commit
    txn.commit().await.unwrap();

    // Query from database level - should now see committed data
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 2, "Should see committed data");
}

/// Test transaction isolation - concurrent transaction doesn't see uncommitted data
#[tokio::test]
async fn test_transaction_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_isolation");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Transaction 1: insert but don't commit yet
    let txn1 = db.begin_transaction().await.unwrap();
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();
    txn1.insert(batch1).await.unwrap();

    // Transaction 2: query - should NOT see txn1's uncommitted data
    let txn2 = db.begin_transaction().await.unwrap();
    let results = txn2
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count_array.value(0),
        0,
        "Should NOT see uncommitted data from other transaction"
    );

    // Commit txn1
    txn1.commit().await.unwrap();

    // New query in txn2 should still not see committed data (snapshot isolation)
    let results = txn2
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count_array.value(0),
        0,
        "Snapshot isolation: txn2 started before txn1 committed"
    );

    txn2.commit().await.unwrap();

    // New transaction should see txn1's committed data
    let txn3 = db.begin_transaction().await.unwrap();
    let results = txn3
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count_array.value(0),
        1,
        "New transaction should see previously committed data"
    );
    txn3.commit().await.unwrap();
}

/// Test delete within transaction
#[tokio::test]
async fn test_transaction_with_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_txn_delete");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let db = Arc::new(
        DatabaseOps::create(db_path.clone(), schema.clone())
            .await
            .unwrap(),
    );

    // Insert initial data
    let txn1 = db.begin_transaction().await.unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "active", "inactive", "active", "inactive",
            ])),
        ],
    )
    .unwrap();
    txn1.insert(batch).await.unwrap();
    txn1.commit().await.unwrap();

    // Delete within transaction
    let txn2 = db.begin_transaction().await.unwrap();
    txn2.delete_rows_where("status = 'inactive'").await.unwrap();
    txn2.commit().await.unwrap();

    // Verify only active rows remain
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 2, "Should have 2 active rows");
}
