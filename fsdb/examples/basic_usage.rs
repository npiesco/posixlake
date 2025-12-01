//! Example 1: Basic Usage - Create database, insert rows, query
//!
//! This example demonstrates:
//! - Creating a new database with a schema
//! - Inserting data using RecordBatch
//! - Querying data with SQL
//! - Basic CRUD operations

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .init();

    println!("\n=== FSDB Basic Usage Example ===\n");

    // 1. Define schema
    println!("1. Creating database schema...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    println!("   Schema: id (Int32), name (Utf8), age (Int32), city (Utf8)");

    // 2. Create database
    println!("\n2. Creating database at /tmp/fsdb_example_basic...");
    let db_path = "/tmp/fsdb_example_basic";
    let _ = std::fs::remove_dir_all(db_path); // Clean up from previous runs

    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    println!("   Database created successfully!");

    // 3. Insert data
    println!("\n3. Inserting data...");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![30, 25, 35, 28, 32])) as ArrayRef,
            Arc::new(StringArray::from(vec!["NYC", "SF", "LA", "NYC", "SF"])) as ArrayRef,
        ],
    )?;

    let txn_id = db.insert(batch).await?;
    println!("   Inserted 5 rows in transaction {}", txn_id);

    // 4. Query all data
    println!("\n4. Querying all data...");
    let results = db.query("SELECT * FROM data ORDER BY id").await?;
    println!("   Query returned {} batches", results.len());

    for (i, batch) in results.iter().enumerate() {
        println!("   Batch {}: {} rows", i, batch.num_rows());
    }

    // 5. Query with WHERE clause
    println!("\n5. Querying with WHERE clause (age > 30)...");
    let results = db
        .query("SELECT name, age, city FROM data WHERE age > 30 ORDER BY age")
        .await?;

    for batch in &results {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let cities = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            println!(
                "   {} (age {}) from {}",
                names.value(i),
                ages.value(i),
                cities.value(i)
            );
        }
    }

    // 6. Aggregation query
    println!("\n6. Aggregation query (count by city)...");
    let results = db
        .query("SELECT city, COUNT(*) as count FROM data GROUP BY city ORDER BY count DESC")
        .await?;

    for batch in &results {
        let cities = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            println!("   {}: {} people", cities.value(i), counts.value(i));
        }
    }

    // 7. Insert more data
    println!("\n7. Inserting additional data...");
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![6, 7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Frank", "Grace"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![29, 31])) as ArrayRef,
            Arc::new(StringArray::from(vec!["LA", "NYC"])) as ArrayRef,
        ],
    )?;

    let txn_id2 = db.insert(batch2).await?;
    println!("   Inserted 2 more rows in transaction {}", txn_id2);

    // 8. Final count
    println!("\n8. Final row count...");
    let results = db.query("SELECT COUNT(*) as total FROM data").await?;

    for batch in &results {
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        println!("   Total rows: {}", count.value(0));
    }

    println!("\n=== Example Complete! ===\n");
    println!("Database location: {}", db_path);
    println!("You can inspect the Delta Lake files:");
    println!("  - Delta transaction log: {}/_delta_log/", db_path);
    println!("  - Parquet data files: {}/*.parquet", db_path);
    println!("\nThis database is readable by:");
    println!("  - Apache Spark with Delta Lake");
    println!("  - Databricks");
    println!("  - AWS Athena");
    println!("  - Any Delta Lake compatible tool");

    Ok(())
}
