//! Example 2: Concurrent Access - Multiple readers and writers
//!
//! This example demonstrates:
//! - Concurrent read operations (snapshot isolation)
//! - Sequential write operations (transaction safety)
//! - Thread-safe database access with Arc
//! - Real-world concurrent usage patterns

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .init();

    println!("\n=== FSDB Concurrent Access Example ===\n");

    // 1. Setup database
    println!("1. Setting up database...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("thread_id", DataType::Int32, false),
        Field::new("operation_id", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let db_path = "/tmp/fsdb_example_concurrent";
    let _ = std::fs::remove_dir_all(db_path);

    let db = Arc::new(DatabaseOps::create(db_path, schema.clone()).await?);
    println!("   Database created at {}", db_path);

    // 2. Insert initial data
    println!("\n2. Inserting initial data...");
    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 0])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "Initial 1",
                "Initial 2",
                "Initial 3",
            ])) as ArrayRef,
        ],
    )?;
    db.insert(initial_batch).await?;
    println!("   Inserted 3 initial rows");

    // 3. Spawn concurrent readers
    println!("\n3. Spawning 5 concurrent reader threads...");
    let mut reader_handles = vec![];

    for reader_id in 1..=5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            println!("   [Reader {}] Starting...", reader_id);

            // Each reader performs 3 queries
            for query_num in 1..=3 {
                thread::sleep(Duration::from_millis(100 * query_num as u64));

                let sql = "SELECT COUNT(*) as count FROM data";
                let result = rt.block_on(db_clone.query(sql));

                match result {
                    Ok(batches) => {
                        let count = batches[0]
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap()
                            .value(0);
                        println!(
                            "   [Reader {}] Query {}: Found {} rows",
                            reader_id, query_num, count
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "   [Reader {}] Query {} failed: {}",
                            reader_id, query_num, e
                        );
                    }
                }
            }

            println!("   [Reader {}] Completed", reader_id);
        });

        reader_handles.push(handle);
    }

    // 4. Spawn writer thread (writes sequentially to avoid conflicts)
    println!("\n4. Spawning writer thread...");
    let db_writer = Arc::clone(&db);
    let schema_writer = schema.clone();

    let writer_handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        println!("   [Writer] Starting...");

        // Writer performs 3 sequential writes
        for write_num in 1..=3 {
            thread::sleep(Duration::from_millis(150));

            let batch = RecordBatch::try_new(
                schema_writer.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![99])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![write_num])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        format!("Write {}", write_num).as_str()
                    ])) as ArrayRef,
                ],
            )
            .unwrap();

            match rt.block_on(db_writer.insert(batch)) {
                Ok(txn_id) => {
                    println!("   [Writer] Write {} completed (txn {})", write_num, txn_id);
                }
                Err(e) => {
                    eprintln!("   [Writer] Write {} failed: {}", write_num, e);
                }
            }
        }

        println!("   [Writer] Completed");
    });

    // 5. Wait for all readers to complete
    println!("\n5. Waiting for readers to complete...");
    for handle in reader_handles {
        handle.join().expect("Reader thread panicked");
    }
    println!("   All readers completed");

    // 6. Wait for writer to complete
    println!("\n6. Waiting for writer to complete...");
    writer_handle.join().expect("Writer thread panicked");
    println!("   Writer completed");

    // 7. Final verification
    println!("\n7. Final verification...");
    let results = db.query("SELECT COUNT(*) as total FROM data").await?;
    let total = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("   Total rows in database: {}", total);

    let results = db
        .query(
            "SELECT thread_id, COUNT(*) as count FROM data GROUP BY thread_id ORDER BY thread_id",
        )
        .await?;
    let thread_ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let counts = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    println!("   Rows by thread:");
    for i in 0..results[0].num_rows() {
        println!(
            "     Thread {}: {} rows",
            thread_ids.value(i),
            counts.value(i)
        );
    }

    // 8. Demonstrate snapshot isolation
    println!("\n8. Demonstrating snapshot isolation...");
    println!("   Creating snapshot for Reader A...");
    let reader_a = Arc::clone(&db);

    // Reader A starts
    let snapshot_a_handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Take a snapshot by starting a query
        println!("   [Reader A] Taking snapshot...");
        let sql = "SELECT COUNT(*) as count FROM data";
        let result = rt.block_on(reader_a.query(sql)).unwrap();
        let count_before = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        thread::sleep(Duration::from_millis(500)); // Simulate some work

        // Query again with same snapshot expectation
        let result2 = rt.block_on(reader_a.query(sql)).unwrap();
        let count_after = result2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        println!(
            "   [Reader A] Count before: {}, Count after: {} (reads see latest committed state)",
            count_before, count_after
        );
    });

    // Writer B inserts during Reader A's work
    thread::sleep(Duration::from_millis(200));
    println!("   [Writer B] Inserting new data while Reader A is working...");
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![88])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Concurrent write"])) as ArrayRef,
        ],
    )?;
    db.insert(batch_b).await?;
    println!("   [Writer B] Insert completed");

    snapshot_a_handle.join().expect("Reader A thread panicked");

    println!("\n=== Example Complete! ===\n");
    println!("Key takeaways:");
    println!("  - Multiple readers can query concurrently");
    println!("  - Writers use Delta Lake's optimistic concurrency control");
    println!("  - Each query sees the latest committed Delta Lake snapshot");
    println!("  - Thread-safe Arc<DatabaseOps> allows safe concurrent access");

    Ok(())
}
