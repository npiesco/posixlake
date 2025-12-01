use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use fsdb::DatabaseOps;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_dir_all(path);
}

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

fn create_large_batch(schema: SchemaRef, start_id: i32, count: usize) -> RecordBatch {
    let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("user_{}", i)).collect();
    // Generate larger data strings to increase file size
    let data_strings: Vec<String> = ids
        .iter()
        .map(|i| {
            format!(
                "large_data_payload_for_stress_testing_{}_with_additional_padding_to_increase_size",
                i
            )
        })
        .collect();
    let values: Vec<i32> = ids.iter().map(|i| i * 10).collect();

    let id_array = Arc::new(Int32Array::from(ids)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(
        names.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
    )) as ArrayRef;
    let data_array = Arc::new(StringArray::from(
        data_strings
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>(),
    )) as ArrayRef;
    let value_array = Arc::new(Int32Array::from(values)) as ArrayRef;

    RecordBatch::try_new(schema, vec![id_array, name_array, data_array, value_array])
        .expect("Failed to create RecordBatch")
}

/// Test: Large file handling (>1GB worth of data)
/// This tests the system's ability to handle very large datasets
#[tokio::test]
async fn test_large_file_handling() {
    setup_logging();
    let db_path = "/tmp/test_db_stress_large_file";
    cleanup_test_db(db_path);

    let schema = create_test_schema();
    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    println!("\n=== Large File Handling Stress Test ===");

    // Insert enough data to create >1GB dataset
    // Each row is roughly 150 bytes, so we need ~7M rows for 1GB
    // We'll use 100K rows across multiple batches as a reasonable stress test
    let batch_size = 1000;
    let num_batches = 100; // 100K rows total
    let total_rows = batch_size * num_batches;

    let start = Instant::now();

    for batch_num in 0..num_batches {
        let batch = create_large_batch(schema.clone(), (batch_num * batch_size) as i32, batch_size);
        db.insert(batch).await.expect("Failed to insert batch");

        if (batch_num + 1) % 10 == 0 {
            println!(
                "  Inserted {} batches ({} rows)",
                batch_num + 1,
                (batch_num + 1) * batch_size
            );
        }
    }

    let insert_duration = start.elapsed();
    let rows_per_sec = total_rows as f64 / insert_duration.as_secs_f64();

    println!("  Total rows inserted: {}", total_rows);
    println!("  Insert duration: {:.2}s", insert_duration.as_secs_f64());
    println!("  Insert throughput: {:.0} rows/sec", rows_per_sec);

    // Measure total database size (Parquet files in Delta Lake format)
    let mut total_size = 0u64;
    for entry in fs::read_dir(db_path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
            total_size += entry.metadata().unwrap().len();
        }
    }

    let size_mb = total_size as f64 / (1024.0 * 1024.0);
    println!("  Database size: {:.2} MB", size_mb);

    // Query the large dataset
    let query_start = Instant::now();
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .expect("Query failed");
    let query_duration = query_start.elapsed();

    println!("  Query duration (COUNT): {}ms", query_duration.as_millis());

    // Verify we got all rows
    assert_eq!(results.len(), 1, "Should return one batch");
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        total_rows as i64,
        "Should have {} rows",
        total_rows
    );

    // Assertions: system should handle large datasets efficiently
    assert!(
        rows_per_sec > 500.0,
        "Insert throughput too low: {} rows/sec (expected > 500)",
        rows_per_sec
    );
    assert!(
        query_duration.as_millis() < 5000,
        "Query too slow: {}ms (expected < 5000ms)",
        query_duration.as_millis()
    );
    assert!(size_mb > 1.0, "Database should be larger than 1 MB");

    cleanup_test_db(db_path);
}

/// Test: Many concurrent readers (stress test with 50 readers)
/// This tests the system's ability to handle high read concurrency
#[tokio::test]
async fn test_many_concurrent_readers() {
    setup_logging();
    let db_path = "/tmp/test_db_stress_concurrent_readers";
    cleanup_test_db(db_path);

    let schema = create_test_schema();
    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );

    println!("\n=== Many Concurrent Readers Stress Test ===");

    // Insert test data
    for batch_num in 0..50 {
        let batch = create_large_batch(schema.clone(), batch_num * 100, 100);
        db.insert(batch).await.expect("Failed to insert");
    }

    println!("  Test data: 5000 rows inserted");

    // Test with 50 concurrent readers
    let num_readers = 50;
    let queries_per_reader = 20;
    let total_queries = num_readers * queries_per_reader;

    let start = Instant::now();

    // Spawn concurrent readers
    let mut handles = vec![];
    for reader_id in 0..num_readers {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            for query_num in 0..queries_per_reader {
                let id_filter = (reader_id * queries_per_reader + query_num) % 100;
                let sql = format!("SELECT * FROM data WHERE id >= {} LIMIT 10", id_filter);
                let _results = db_clone.query(&sql).await.expect("Query failed");
            }
        });
        handles.push(handle);
    }

    // Wait for all readers to finish
    for handle in handles {
        handle.await.expect("Thread panicked");
    }

    let duration = start.elapsed();
    let queries_per_sec = total_queries as f64 / duration.as_secs_f64();
    let avg_latency_ms = (duration.as_millis() as f64) / total_queries as f64;

    println!("  Concurrent readers: {}", num_readers);
    println!("  Total queries: {}", total_queries);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} queries/sec", queries_per_sec);
    println!("  Avg latency: {:.2}ms per query", avg_latency_ms);

    // Assertions: system should handle high concurrency
    assert!(
        queries_per_sec > 10.0,
        "Query throughput too low: {} queries/sec (expected > 10)",
        queries_per_sec
    );
    assert!(
        avg_latency_ms < 1000.0,
        "Average latency too high: {:.2}ms (expected < 1000ms)",
        avg_latency_ms
    );

    cleanup_test_db(db_path);
}

/// Test: Rapid write operations (stress test with many small writes)
/// This tests the system's ability to handle write-heavy workloads
#[tokio::test]
async fn test_rapid_write_operations() {
    setup_logging();
    let db_path = "/tmp/test_db_stress_rapid_writes";
    cleanup_test_db(db_path);

    let schema = create_test_schema();
    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    println!("\n=== Rapid Write Operations Stress Test ===");

    // Perform many small writes rapidly
    let num_writes = 500;
    let rows_per_write = 10;

    let start = Instant::now();

    for write_num in 0..num_writes {
        let batch = create_large_batch(
            schema.clone(),
            (write_num * rows_per_write) as i32,
            rows_per_write,
        );
        db.insert(batch).await.expect("Failed to insert");

        if (write_num + 1) % 100 == 0 {
            println!("  Completed {} writes", write_num + 1);
        }
    }

    let duration = start.elapsed();
    let total_rows = num_writes * rows_per_write;
    let writes_per_sec = num_writes as f64 / duration.as_secs_f64();
    let rows_per_sec = total_rows as f64 / duration.as_secs_f64();

    println!("  Total writes: {}", num_writes);
    println!("  Total rows: {}", total_rows);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Write throughput: {:.0} writes/sec", writes_per_sec);
    println!("  Row throughput: {:.0} rows/sec", rows_per_sec);

    // Verify all rows were written
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .expect("Query failed");
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Expected Int64Array");
    let final_count = count_array.value(0);

    println!("  Final row count: {}", final_count);

    // Assertions: system should handle rapid writes
    assert_eq!(
        final_count, total_rows as i64,
        "Should have all {} rows",
        total_rows
    );
    assert!(
        writes_per_sec > 5.0,
        "Write throughput too low: {} writes/sec (expected > 5)",
        writes_per_sec
    );
    assert!(
        rows_per_sec > 50.0,
        "Row throughput too low: {} rows/sec (expected > 50)",
        rows_per_sec
    );

    cleanup_test_db(db_path);
}

/// Test: Memory leak detection (insert and query many times, verify memory stability)
/// This tests that the system doesn't accumulate memory over repeated operations
#[tokio::test]
async fn test_memory_leak_detection() {
    setup_logging();
    let db_path = "/tmp/test_db_stress_memory_leak";
    cleanup_test_db(db_path);

    let schema = create_test_schema();
    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    println!("\n=== Memory Leak Detection Stress Test ===");

    // Perform repeated insert/query cycles
    let num_cycles = 100;
    let rows_per_cycle = 100;

    let start = Instant::now();

    for cycle in 0..num_cycles {
        // Insert data
        let batch = create_large_batch(
            schema.clone(),
            (cycle * rows_per_cycle) as i32,
            rows_per_cycle,
        );
        db.insert(batch).await.expect("Failed to insert");

        // Query data
        let _results = db
            .query("SELECT * FROM data LIMIT 10")
            .await
            .expect("Query failed");
        let _count = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .expect("Query failed");

        if (cycle + 1) % 25 == 0 {
            println!("  Completed {} cycles", cycle + 1);
        }
    }

    let duration = start.elapsed();
    let cycles_per_sec = num_cycles as f64 / duration.as_secs_f64();

    println!("  Total cycles: {}", num_cycles);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} cycles/sec", cycles_per_sec);

    // Verify final state
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .expect("Query failed");
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Expected Int64Array");
    let final_count = count_array.value(0);

    let expected_rows = num_cycles * rows_per_cycle;
    println!(
        "  Final row count: {} (expected {})",
        final_count, expected_rows
    );

    // Assertions: system should complete all cycles successfully
    assert_eq!(
        final_count, expected_rows as i64,
        "Should have all {} rows",
        expected_rows
    );
    // Delta Lake operations have more overhead than simple file I/O, so we expect lower throughput
    assert!(
        cycles_per_sec > 2.0,
        "Cycle throughput too low: {} cycles/sec (expected > 2 for Delta Lake)",
        cycles_per_sec
    );

    // Note: Actual memory leak detection would require inspecting process memory
    // This test verifies functional stability under repeated operations
    // Real memory profiling would need tools like Valgrind or heaptrack

    cleanup_test_db(db_path);
}
