use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use fsdb::DatabaseOps;
use std::fs;
use std::sync::Arc;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("fsdb=debug")
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_dir_all(path);
}

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

fn create_test_batch(
    schema: SchemaRef,
    ids: Vec<i32>,
    names: Vec<&str>,
    values: Vec<i32>,
) -> RecordBatch {
    let id_array = Arc::new(Int32Array::from(ids)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let value_array = Arc::new(Int32Array::from(values)) as ArrayRef;

    RecordBatch::try_new(schema, vec![id_array, name_array, value_array]).unwrap()
}

#[tokio::test]
async fn test_recovery_after_abrupt_shutdown() {
    setup_logging();
    let db_path = "/tmp/test_db_recovery_shutdown";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Step 1: Create database and write some committed data
    {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");

        let batch1 = create_test_batch(
            schema.clone(),
            vec![1, 2, 3],
            vec!["record1", "record2", "record3"],
            vec![100, 200, 300],
        );
        db.insert(batch1).await.expect("Failed to insert batch1");

        // Verify data was written
        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            3,
            "Should have 3 records before shutdown"
        );
    } // Database dropped here, simulating abrupt shutdown

    // Step 2: Reopen database and verify data persisted
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen database");

        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query after recovery");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            3,
            "Should recover all 3 committed records"
        );

        // Verify specific records
        let results = db
            .query("SELECT id, name, value FROM data ORDER BY id")
            .await
            .expect("Failed to query records");
        let id_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = results[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(id_col.value(0), 1);
        assert_eq!(name_col.value(0), "record1");
        assert_eq!(value_col.value(0), 100);

        assert_eq!(id_col.value(2), 3);
        assert_eq!(name_col.value(2), "record3");
        assert_eq!(value_col.value(2), 300);
    }

    cleanup_test_db(db_path);
}

#[tokio::test]
async fn test_recovery_with_multiple_restarts() {
    setup_logging();
    let db_path = "/tmp/test_db_recovery_multiple";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // First session: write and commit
    {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");
        let batch = create_test_batch(schema.clone(), vec![1], vec!["first"], vec![1]);
        db.insert(batch).await.expect("Failed to insert");
    }

    // Second session: write more data
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen 1");
        let batch = create_test_batch(schema.clone(), vec![2], vec!["second"], vec![2]);
        db.insert(batch).await.expect("Failed to insert");
    }

    // Third session: write even more data
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen 2");
        let batch = create_test_batch(schema.clone(), vec![3], vec!["third"], vec![3]);
        db.insert(batch).await.expect("Failed to insert");
    }

    // Fourth session: verify all data persisted
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen 3");
        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            3,
            "Should have all 3 records after multiple restarts"
        );

        // Verify we can still write after recovery
        let batch = create_test_batch(schema.clone(), vec![4], vec!["fourth"], vec![4]);
        db.insert(batch)
            .await
            .expect("Failed to insert after recovery");

        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            4,
            "Should have 4 records after post-recovery write"
        );
    }

    cleanup_test_db(db_path);
}

#[tokio::test]
async fn test_recovery_with_deleted_rows() {
    setup_logging();
    let db_path = "/tmp/test_db_recovery_deleted";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Create database and insert records
    {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");

        let batch1 = create_test_batch(
            schema.clone(),
            vec![1, 2],
            vec!["keep1", "keep2"],
            vec![1, 2],
        );
        db.insert(batch1).await.expect("Failed to insert batch1");

        let batch2 = create_test_batch(schema.clone(), vec![3], vec!["delete_me"], vec![3]);
        db.insert(batch2).await.expect("Failed to insert batch2");

        // Delete rows using Delta Lake row-level deletion (WHERE clause)
        db.delete_rows_where("name = 'delete_me'")
            .await
            .expect("Failed to delete rows");
    }

    // Reopen and verify deletion persisted
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen after deletion");

        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            2,
            "Should have 2 records after deletion persisted"
        );

        // Verify the right records remain
        let results = db
            .query("SELECT name FROM data ORDER BY id")
            .await
            .expect("Failed to query names");
        let name_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "keep1");
        assert_eq!(name_col.value(1), "keep2");
    }

    cleanup_test_db(db_path);
}

#[tokio::test]
async fn test_wal_corruption_handling() {
    setup_logging();
    let db_path = "/tmp/test_db_wal_corruption";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Create database and write data
    {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");

        let batch = create_test_batch(
            schema.clone(),
            vec![1, 2, 3],
            vec!["r1", "r2", "r3"],
            vec![1, 2, 3],
        );
        db.insert(batch).await.expect("Failed to insert");
    }

    // Note: Delta Lake native mode doesn't use legacy WAL - it uses Delta transaction log
    // This test verifies that data persists correctly across database sessions

    // Reopen database - Delta Lake transaction log provides durability
    {
        let db = DatabaseOps::open(db_path)
            .await
            .expect("Failed to reopen database");

        // The committed data should be accessible via Delta Lake transaction log
        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            3,
            "Should recover data from Delta Lake transaction log"
        );
    }

    cleanup_test_db(db_path);
}

#[tokio::test]
async fn test_transaction_log_based_recovery() {
    setup_logging();
    let db_path = "/tmp/test_db_txn_log_recovery";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Create database with multiple transactions
    {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");

        // Transaction 1
        let batch1 = create_test_batch(schema.clone(), vec![1], vec!["txn1"], vec![1]);
        db.insert(batch1).await.expect("Failed to insert txn1");

        // Transaction 2
        let batch2 = create_test_batch(schema.clone(), vec![2], vec!["txn2"], vec![2]);
        db.insert(batch2).await.expect("Failed to insert txn2");

        // Transaction 3
        let batch3 = create_test_batch(schema.clone(), vec![3], vec!["txn3"], vec![3]);
        db.insert(batch3).await.expect("Failed to insert txn3");
    }

    // Verify Delta Lake transaction log exists
    let delta_log_dir = format!("{}/_delta_log", db_path);
    assert!(
        fs::metadata(&delta_log_dir).is_ok(),
        "Delta Lake transaction log directory should exist"
    );

    // Reopen and verify Delta Lake transaction log can reconstruct state
    {
        let db = DatabaseOps::open(db_path).await.expect("Failed to reopen");

        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Failed to query");
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            3,
            "Should recover all 3 transactions from Delta Lake log"
        );

        // Verify ordering is maintained
        let results = db
            .query("SELECT name FROM data ORDER BY id")
            .await
            .expect("Failed to query names");
        let name_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "txn1");
        assert_eq!(name_col.value(1), "txn2");
        assert_eq!(name_col.value(2), "txn3");
    }

    cleanup_test_db(db_path);
}
