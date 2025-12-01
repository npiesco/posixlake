use arrow::datatypes::{DataType, Field, Schema};
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_query_with_multiple_parquet_files_schema_mismatch() {
    // Create database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert data in first transaction (creates first Parquet file)
    let batch1 = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Charlie",
            ])),
            Arc::new(arrow::array::Int32Array::from(vec![25, 30, 35])),
            Arc::new(arrow::array::StringArray::from(vec!["NYC", "SF", "LA"])),
        ],
    )
    .unwrap();

    db.insert(batch1).await.unwrap();

    // Insert data in second transaction (creates second Parquet file)
    let batch2 = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![4, 5])),
            Arc::new(arrow::array::StringArray::from(vec!["David", "Eve"])),
            Arc::new(arrow::array::Int32Array::from(vec![28, 32])),
            Arc::new(arrow::array::StringArray::from(vec!["NYC", "SF"])),
        ],
    )
    .unwrap();

    db.insert(batch2).await.unwrap();

    // Drop the database to close it
    drop(db);

    // Reopen the database from disk (like what happens during mount)
    let db = DatabaseOps::open(&db_path).await.unwrap();

    // Check what schema was loaded
    let loaded_schema = db.schema();
    println!(
        "Loaded schema after reopen: {} fields",
        loaded_schema.fields().len()
    );
    println!(
        "Schema fields: {:?}",
        loaded_schema
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    // Now try to query all data (this should trigger the schema mismatch error)
    let result = db.query("SELECT * FROM data").await;

    match result {
        Ok(batches) => {
            println!("Query succeeded! Returned {} batches", batches.len());
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("Total rows: {}", total_rows);
            assert_eq!(total_rows, 5, "Should have 5 rows total");
        }
        Err(e) => {
            panic!("Query failed with error: {}", e);
        }
    }
}
