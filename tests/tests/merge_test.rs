//! Delta Lake MERGE Integration Tests
//!
//! These tests validate the MERGE operation (INSERT + UPDATE + DELETE in single transaction)
//! All tests use real Delta Lake operations, no mocks.

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

/// Test 1: Simple MERGE - INSERT only (new rows that don't exist)
/// This validates the WHEN NOT MATCHED INSERT clause
#[tokio::test]
async fn test_merge_insert_only() {
    println!("\n[TEST] test_merge_insert_only - INSERT new rows via MERGE");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_merge_insert");

    // Create initial table
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone())
        .await
        .unwrap();

    // Insert initial data
    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![25, 30, 35])),
        ],
    )
    .unwrap();
    db.insert(initial_batch).await.unwrap();
    println!("[SETUP] Inserted 3 initial rows");

    // Verify initial state
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Should have 3 rows initially");

    // Prepare source data with NEW rows only (IDs 4, 5)
    let source_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["David", "Eve"])),
            Arc::new(Int32Array::from(vec![28, 32])),
        ],
    )
    .unwrap();

    println!("[MERGE] Executing MERGE to insert new rows");

    // Execute MERGE
    let merge_start = std::time::Instant::now();
    let metrics = db
        .merge()
        .await
        .unwrap()
        .with_source(source_batch, "source")
        .on("target.id = source.id")
        .when_not_matched_insert()
        .values_all()
        .execute()
        .await
        .unwrap();
    let merge_duration = merge_start.elapsed();

    println!("[RESULT] MERGE completed in {:?}", merge_duration);
    println!("[RESULT] Rows inserted: {}", metrics.rows_inserted);
    println!("[RESULT] Rows updated: {}", metrics.rows_updated);
    println!("[RESULT] Rows deleted: {}", metrics.rows_deleted);

    // Verify: should have 5 rows now (3 initial + 2 new)
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 5, "Should have 5 rows after MERGE");
    assert_eq!(metrics.rows_inserted, 2, "Should have inserted 2 rows");
    assert_eq!(metrics.rows_updated, 0, "Should have updated 0 rows");
    assert_eq!(metrics.rows_deleted, 0, "Should have deleted 0 rows");

    // Verify the new rows exist
    let results = db
        .query("SELECT * FROM data WHERE id IN (4, 5) ORDER BY id")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 2, "Should find both new rows");

    let ids = results[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 4);
    assert_eq!(ids.value(1), 5);

    let names = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "David");
    assert_eq!(names.value(1), "Eve");

    println!("[SUCCESS] MERGE INSERT test passed");
}

/// Test 2: MERGE UPDATE - Update existing rows
/// This validates the WHEN MATCHED UPDATE clause
#[tokio::test]
async fn test_merge_update_only() {
    println!("\n[TEST] test_merge_update_only - UPDATE existing rows via MERGE");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_merge_update");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone())
        .await
        .unwrap();

    // Insert initial data
    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![25, 30, 35])),
        ],
    )
    .unwrap();
    db.insert(initial_batch).await.unwrap();
    println!("[SETUP] Inserted 3 initial rows");

    // Prepare source data with UPDATED values for existing IDs
    let source_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice Updated", "Bob Updated"])),
            Arc::new(Int32Array::from(vec![26, 31])),
        ],
    )
    .unwrap();

    println!("[MERGE] Executing MERGE to update existing rows");

    let merge_start = std::time::Instant::now();
    let metrics = db
        .merge()
        .await
        .unwrap()
        .with_source(source_batch, "source")
        .on("target.id = source.id")
        .when_matched_update()
        .set_all()
        .execute()
        .await
        .unwrap();
    let merge_duration = merge_start.elapsed();

    println!("[RESULT] MERGE completed in {:?}", merge_duration);
    println!("[RESULT] Rows updated: {}", metrics.rows_updated);

    // Verify: should still have 3 rows
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 3, "Should still have 3 rows after UPDATE");
    assert_eq!(metrics.rows_updated, 2, "Should have updated 2 rows");
    assert_eq!(metrics.rows_inserted, 0, "Should have inserted 0 rows");
    assert_eq!(metrics.rows_deleted, 0, "Should have deleted 0 rows");

    // Verify the updated values
    let results = db
        .query("SELECT * FROM data WHERE id IN (1, 2) ORDER BY id")
        .await
        .unwrap();
    let names = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice Updated");
    assert_eq!(names.value(1), "Bob Updated");

    let ages = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 26);
    assert_eq!(ages.value(1), 31);

    println!("[SUCCESS] MERGE UPDATE test passed");
}

/// Test 3: MERGE DELETE - Delete matching rows
/// This validates the WHEN MATCHED DELETE clause
#[tokio::test]
async fn test_merge_delete_only() {
    println!("\n[TEST] test_merge_delete_only - DELETE rows via MERGE");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_merge_delete");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone())
        .await
        .unwrap();

    // Insert initial data
    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )
    .unwrap();
    db.insert(initial_batch).await.unwrap();
    println!("[SETUP] Inserted 5 initial rows");

    // Prepare source data with IDs to DELETE (2, 4)
    let source_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec!["Bob", "David"])),
            Arc::new(Int32Array::from(vec![30, 28])),
        ],
    )
    .unwrap();

    println!("[MERGE] Executing MERGE to delete rows");

    let merge_start = std::time::Instant::now();
    let metrics = db
        .merge()
        .await
        .unwrap()
        .with_source(source_batch, "source")
        .on("target.id = source.id")
        .when_matched_delete()
        .then()
        .execute()
        .await
        .unwrap();
    let merge_duration = merge_start.elapsed();

    println!("[RESULT] MERGE completed in {:?}", merge_duration);
    println!("[RESULT] Rows deleted: {}", metrics.rows_deleted);

    // Verify: should have 3 rows remaining (1, 3, 5)
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 3, "Should have 3 rows after DELETE");
    assert_eq!(metrics.rows_deleted, 2, "Should have deleted 2 rows");
    assert_eq!(metrics.rows_inserted, 0, "Should have inserted 0 rows");
    assert_eq!(metrics.rows_updated, 0, "Should have updated 0 rows");

    // Verify the remaining rows
    let results = db.query("SELECT id FROM data ORDER BY id").await.unwrap();
    let ids = results[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
    assert_eq!(ids.value(2), 5);

    println!("[SUCCESS] MERGE DELETE test passed");
}

/// Test 4: MERGE with all operations (INSERT + UPDATE + DELETE)
/// This is the comprehensive test that validates full MERGE functionality
#[tokio::test]
async fn test_merge_full_upsert() {
    println!("\n[TEST] test_merge_full_upsert - INSERT + UPDATE + DELETE in single MERGE");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_merge_full");

    // Schema with _op column to control operations
    let target_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let source_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("_op", DataType::Utf8, false), // Operation type
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create_with_delta_native(db_path.clone(), target_schema.clone())
        .await
        .unwrap();

    // Insert initial data
    let initial_batch = RecordBatch::try_new(
        target_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 28])),
        ],
    )
    .unwrap();
    db.insert(initial_batch).await.unwrap();
    println!("[SETUP] Inserted 4 initial rows: Alice(1), Bob(2), Charlie(3), David(4)");

    // Prepare source data with mixed operations:
    // - UPDATE: Alice (id=1) age changed to 26
    // - DELETE: Bob (id=2)
    // - INSERT: Eve (id=5, new row)
    // - No change: Charlie (id=3) not in source, should remain
    // - No change: David (id=4) not in source, should remain
    let source_batch = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 5])),
            Arc::new(StringArray::from(vec!["Alice Updated", "Bob", "Eve"])),
            Arc::new(Int32Array::from(vec![26, 30, 32])),
            Arc::new(StringArray::from(vec!["UPDATE", "DELETE", "INSERT"])),
        ],
    )
    .unwrap();

    println!("[MERGE] Executing full MERGE with INSERT + UPDATE + DELETE");

    let merge_start = std::time::Instant::now();
    let metrics = db
        .merge()
        .await
        .unwrap()
        .with_source(source_batch, "source")
        .on("target.id = source.id")
        .when_matched_update()
        .condition("source._op = 'UPDATE'")
        .set_all()
        .when_matched_delete()
        .condition("source._op = 'DELETE'")
        .then()
        .when_not_matched_insert()
        .condition("source._op = 'INSERT'")
        .values_all()
        .execute()
        .await
        .unwrap();
    let merge_duration = merge_start.elapsed();

    println!("[RESULT] MERGE completed in {:?}", merge_duration);
    println!("[RESULT] Rows inserted: {}", metrics.rows_inserted);
    println!("[RESULT] Rows updated: {}", metrics.rows_updated);
    println!("[RESULT] Rows deleted: {}", metrics.rows_deleted);
    println!("[RESULT] Total affected: {}", metrics.total_rows_affected());

    // Verify metrics
    assert_eq!(metrics.rows_inserted, 1, "Should have inserted 1 row (Eve)");
    assert_eq!(metrics.rows_updated, 1, "Should have updated 1 row (Alice)");
    assert_eq!(metrics.rows_deleted, 1, "Should have deleted 1 row (Bob)");

    // Verify final row count: started with 4, deleted 1, inserted 1 = 4 total
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 4, "Should have 4 rows after MERGE");

    // Verify specific rows
    // Alice should be updated
    let results = db
        .query("SELECT name, age FROM data WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1);
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Alice Updated", "Alice should be updated");
    assert_eq!(age, 26, "Alice's age should be updated");

    // Bob should be deleted
    let results = db
        .query("SELECT COUNT(*) as count FROM data WHERE id = 2")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 0, "Bob should be deleted");

    // Charlie should be unchanged
    let results = db
        .query("SELECT name FROM data WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1);
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Charlie", "Charlie should be unchanged");

    // Eve should be inserted
    let results = db
        .query("SELECT name, age FROM data WHERE id = 5")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1);
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Eve", "Eve should be inserted");
    assert_eq!(age, 32, "Eve's age should be correct");

    println!("[SUCCESS] Full MERGE test passed");
}
