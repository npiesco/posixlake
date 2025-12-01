// Time Travel Integration Tests
// Tests Delta Lake time travel features: query historical versions

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_time_travel_by_version() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("time_travel_db");

    // Create database
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path.to_str().unwrap(), schema.clone())
        .await
        .expect("Failed to create database");

    // Insert first batch (will be Delta Lake version 1, version 0 is table creation)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["v1_a", "v1_b"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch1).await.expect("Failed to insert batch 1");

    // Verify we have 2 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "After first insert should have 2 rows");

    // Insert second batch (will be Delta Lake version 2)
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["v2_c", "v2_d"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch2).await.expect("Failed to insert batch 2");

    // Current version has 4 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4, "After second insert should have 4 rows");

    // Insert third batch (will be Delta Lake version 3)
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["v3_e", "v3_f"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch3).await.expect("Failed to insert batch 3");

    // Current version has 6 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 6, "After third insert should have 6 rows");

    // TIME TRAVEL: Query version 1 (first insert, should have 2 rows)
    let results_v1 = db
        .query_version("SELECT * FROM data ORDER BY id", 1)
        .await
        .expect("Failed to query version 1");
    let total_rows_v1: usize = results_v1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows_v1, 2,
        "Version 1 (first insert) should return 2 rows"
    );

    // Verify version 1 data content
    let ids = results_v1[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = results_v1[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(values.value(0), "v1_a");
    assert_eq!(ids.value(1), 2);
    assert_eq!(values.value(1), "v1_b");

    // TIME TRAVEL: Query version 2 (second insert, should have 4 rows)
    let results_v2 = db
        .query_version("SELECT COUNT(*) as count FROM data", 2)
        .await
        .expect("Failed to query version 2");
    let count_v2 = results_v2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_v2, 4, "Version 2 (second insert) should have 4 rows");

    // Verify current version still has 6 rows (time travel doesn't affect current state)
    let results_current = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_current = results_current[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_current, 6, "Current version should still have 6 rows");

    println!("✓ Time travel by version working correctly");
}

#[tokio::test]
async fn test_time_travel_by_timestamp() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("time_travel_timestamp_db");

    // Create database
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("event", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path.to_str().unwrap(), schema.clone())
        .await
        .expect("Failed to create database");

    // Insert first batch
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["event_1"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch1).await.expect("Failed to insert batch 1");

    // Capture timestamp AFTER first insert completes
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Wait before second insert
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Insert second batch
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![200])) as ArrayRef,
            Arc::new(StringArray::from(vec!["event_2"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch2).await.expect("Failed to insert batch 2");

    // Capture timestamp AFTER second insert completes
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let t2 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Wait before third insert
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Insert third batch
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![300])) as ArrayRef,
            Arc::new(StringArray::from(vec!["event_3"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch3).await.expect("Failed to insert batch 3");

    // Current state has 3 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Current version should have 3 rows");

    // TIME TRAVEL: Query at t1 (after first insert, should have 1 row)
    let results_t1 = db
        .query_timestamp("SELECT COUNT(*) as count FROM data", t1)
        .await
        .expect("Failed to query at t1");
    let count_t1 = results_t1[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_t1, 1, "At t1 (after first insert) should have 1 row");

    // TIME TRAVEL: Query at t2 (after second insert, should have 2 rows)
    let results_t2 = db
        .query_timestamp("SELECT COUNT(*) as count FROM data", t2)
        .await
        .expect("Failed to query at t2");
    let count_t2 = results_t2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_t2, 2,
        "At t2 (after second insert) should have 2 rows"
    );

    println!("✓ Time travel by timestamp working correctly");
}

#[tokio::test]
async fn test_time_travel_with_deletion() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("time_travel_deletion_db");

    // Create database
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path.to_str().unwrap(), schema.clone())
        .await
        .expect("Failed to create database");

    // Insert 4 rows (will be Delta Lake version 1)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "active", "active", "active", "active",
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch1).await.expect("Failed to insert");

    // Verify 4 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4, "After insert should have 4 rows");

    // Delete 2 rows (id = 2 and id = 4) - will be Delta Lake version 2
    db.delete_rows_where("id = 2 OR id = 4")
        .await
        .expect("Failed to delete rows");

    // Current version has 2 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "After deletion should have 2 rows");

    // TIME TRAVEL: Query version 1 (before deletion, should have 4 rows)
    let results_v1 = db
        .query_version("SELECT COUNT(*) as count FROM data", 1)
        .await
        .expect("Failed to query version 1");
    let count_v1 = results_v1[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_v1, 4,
        "Version 1 (before deletion) should have 4 rows"
    );

    // Verify deleted rows are visible in version 1
    let results_v1_data = db
        .query_version("SELECT * FROM data ORDER BY id", 1)
        .await
        .expect("Failed to query version 1 data");
    let total_rows_v1: usize = results_v1_data.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows_v1, 4, "Version 1 should show all 4 rows");

    println!("✓ Time travel with deletion working correctly");
}
