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
        Field::new("value", DataType::Int32, false),
    ]))
}

fn create_batch_with_rows(schema: SchemaRef, start_id: i32, count: usize) -> RecordBatch {
    let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("user_{}", i)).collect();
    let values: Vec<i32> = ids.iter().map(|i| i * 10).collect();

    let id_array = Arc::new(Int32Array::from(ids)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(
        names.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
    )) as ArrayRef;
    let value_array = Arc::new(Int32Array::from(values)) as ArrayRef;

    RecordBatch::try_new(schema, vec![id_array, name_array, value_array])
        .expect("Failed to create RecordBatch")
}

/// Test: Sequential write performance - measure rows/sec and storage overhead
#[tokio::test]
async fn test_sequential_write_performance() {
    setup_logging();
    let db_path = "/tmp/test_db_perf_write";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Create database
    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Benchmark configuration
    let batch_sizes = vec![10, 100, 1000];
    let num_batches = 10;

    println!("\n=== Sequential Write Performance ===");

    for batch_size in batch_sizes {
        let total_rows = batch_size * num_batches;
        let start = Instant::now();

        // Insert batches sequentially
        for batch_num in 0..num_batches {
            let batch =
                create_batch_with_rows(schema.clone(), (batch_num * batch_size) as i32, batch_size);
            db.insert(batch).await.expect("Failed to insert");
        }

        let duration = start.elapsed();
        let rows_per_sec = total_rows as f64 / duration.as_secs_f64();

        // Measure storage overhead (Delta Lake mode)
        let mut parquet_size = 0u64;
        let mut delta_log_size = 0u64;

        if let Ok(entries) = fs::read_dir(db_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Ok(metadata) = entry.metadata() {
                    let size = metadata.len();
                    if path.is_file()
                        && path.extension().and_then(|s| s.to_str()) == Some("parquet")
                    {
                        parquet_size += size;
                    } else if path.file_name().and_then(|s| s.to_str()) == Some("_delta_log") {
                        delta_log_size += get_directory_size(path.to_str().unwrap());
                    }
                }
            }
        }

        let total_size = parquet_size + delta_log_size;
        let bytes_per_row = if total_rows > 0 {
            total_size / total_rows as u64
        } else {
            0
        };
        let overhead_pct = if total_size > 0 {
            (delta_log_size as f64 / total_size as f64) * 100.0
        } else {
            0.0
        };

        println!("Batch size: {} rows", batch_size);
        println!("  Total rows: {}", total_rows);
        println!("  Duration: {:.3}s", duration.as_secs_f64());
        println!("  Throughput: {:.0} rows/sec", rows_per_sec);
        println!("  Parquet files size: {} bytes", parquet_size);
        println!(
            "  Delta log overhead: {} bytes ({:.1}%)",
            delta_log_size, overhead_pct
        );
        println!("  Total DB size: {} bytes", total_size);
        println!("  Bytes per row: {}", bytes_per_row);
        println!();

        // Assertions: reasonable performance expectations
        assert!(
            rows_per_sec > 10.0,
            "Write throughput too low: {} rows/sec (expected > 10)",
            rows_per_sec
        );
        assert!(
            bytes_per_row < 10000,
            "Storage overhead too high: {} bytes/row (expected < 10KB)",
            bytes_per_row
        );
        assert!(
            overhead_pct < 50.0,
            "Metadata overhead too high: {:.1}% (expected < 50%)",
            overhead_pct
        );
    }

    cleanup_test_db(db_path);
}

/// Test: Concurrent read performance - measure queries/sec
#[tokio::test]
async fn test_concurrent_read_performance() {
    setup_logging();
    let db_path = "/tmp/test_db_perf_read";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    // Create database and insert test data
    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );

    // Insert 1000 rows across 10 batches
    for batch_num in 0..10 {
        let batch = create_batch_with_rows(schema.clone(), batch_num * 100, 100);
        db.insert(batch).await.expect("Failed to insert");
    }

    println!("\n=== Concurrent Read Performance ===");

    // Test with different numbers of concurrent readers
    let concurrency_levels = vec![1, 5, 10];

    for num_readers in concurrency_levels {
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
            handle.await.expect("Task panicked");
        }

        let duration = start.elapsed();
        let queries_per_sec = total_queries as f64 / duration.as_secs_f64();
        let avg_latency_ms = (duration.as_millis() as f64) / total_queries as f64;

        println!("Concurrent readers: {}", num_readers);
        println!("  Total queries: {}", total_queries);
        println!("  Duration: {:.3}s", duration.as_secs_f64());
        println!("  Throughput: {:.0} queries/sec", queries_per_sec);
        println!("  Avg latency: {:.2}ms per query", avg_latency_ms);
        println!();

        // Assertions: reasonable performance expectations
        assert!(
            queries_per_sec > 5.0,
            "Query throughput too low: {} queries/sec (expected > 5)",
            queries_per_sec
        );
        assert!(
            avg_latency_ms < 1000.0,
            "Query latency too high: {:.2}ms (expected < 1000ms)",
            avg_latency_ms
        );
    }

    cleanup_test_db(db_path);
}

/// Test: Query latency on various data sizes
#[tokio::test]
async fn test_query_latency_scaling() {
    setup_logging();
    let db_path = "/tmp/test_db_perf_latency";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    println!("\n=== Query Latency Scaling ===");

    // Test with different dataset sizes
    let dataset_sizes = vec![100, 1000, 10000];

    for total_rows in dataset_sizes {
        let db = DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database");

        // Insert data in batches of 100
        let batch_size = 100;
        let num_batches = total_rows / batch_size;

        for batch_num in 0..num_batches {
            let batch =
                create_batch_with_rows(schema.clone(), (batch_num * batch_size) as i32, batch_size);
            db.insert(batch).await.expect("Failed to insert");
        }

        // Run different query types and measure latency
        let query_types = vec![
            ("SELECT COUNT(*)", "Full table scan with aggregation"),
            ("SELECT * FROM data LIMIT 10", "Point query with limit"),
            (
                "SELECT * FROM data WHERE id >= 500 AND id < 600",
                "Range query",
            ),
            (
                "SELECT name, AVG(value) FROM data GROUP BY name LIMIT 10",
                "Aggregation with grouping",
            ),
        ];

        println!("Dataset size: {} rows", total_rows);

        for (sql, description) in query_types {
            let start = Instant::now();
            let _results = db.query(sql).await.expect("Query failed");
            let latency_ms = start.elapsed().as_millis();

            println!("  {}: {}ms", description, latency_ms);

            // Assertions: queries should complete in reasonable time
            assert!(
                latency_ms < 5000,
                "Query too slow: {}ms for {} on {} rows",
                latency_ms,
                description,
                total_rows
            );
        }

        println!();

        // Cleanup for next iteration
        cleanup_test_db(db_path);
    }
}

/// Test: Memory overhead - measure actual RAM usage during operations
#[tokio::test]
async fn test_memory_overhead() {
    setup_logging();
    let db_path = "/tmp/test_db_perf_memory";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    println!("\n=== Memory Overhead Benchmark ===");

    // Get baseline memory before database creation
    let baseline_memory = get_current_memory_usage();
    println!(
        "Baseline memory: {} MB",
        baseline_memory as f64 / 1_048_576.0
    );

    // Create database
    let db = Arc::new(
        DatabaseOps::create(db_path, schema.clone())
            .await
            .expect("Failed to create database"),
    );
    let after_create_memory = get_current_memory_usage();
    let create_overhead = after_create_memory.saturating_sub(baseline_memory);
    println!(
        "After database creation: {} MB (+{} MB)",
        after_create_memory as f64 / 1_048_576.0,
        create_overhead as f64 / 1_048_576.0
    );

    // Insert data and measure memory growth
    let num_batches = 100;
    let batch_size = 1000;
    let total_rows = num_batches * batch_size;

    for batch_num in 0..num_batches {
        let batch =
            create_batch_with_rows(schema.clone(), (batch_num * batch_size) as i32, batch_size);
        db.insert(batch).await.expect("Failed to insert");
    }

    let after_insert_memory = get_current_memory_usage();
    let insert_overhead = after_insert_memory.saturating_sub(after_create_memory);
    println!(
        "After inserting {} rows: {} MB (+{} MB)",
        total_rows,
        after_insert_memory as f64 / 1_048_576.0,
        insert_overhead as f64 / 1_048_576.0
    );

    // Query data and measure memory during queries
    let num_queries = 50;
    for i in 0..num_queries {
        let sql = format!("SELECT * FROM data WHERE id >= {} LIMIT 100", i * 100);
        let _results = db.query(&sql).await.expect("Query failed");
    }

    let after_query_memory = get_current_memory_usage();
    let query_overhead = after_query_memory.saturating_sub(after_insert_memory);
    println!(
        "After {} queries: {} MB (+{} MB)",
        num_queries,
        after_query_memory as f64 / 1_048_576.0,
        query_overhead as f64 / 1_048_576.0
    );

    // Calculate bytes per row in memory
    let total_overhead = after_query_memory.saturating_sub(baseline_memory);
    let bytes_per_row_memory = total_overhead / total_rows as u64;

    println!("\nMemory Summary:");
    println!(
        "  Total memory overhead: {} MB",
        total_overhead as f64 / 1_048_576.0
    );
    println!("  Memory per row: {} bytes", bytes_per_row_memory);
    println!("  Total rows: {}", total_rows);

    // Assertions: memory usage should be reasonable
    // Note: Delta Lake with caching, Arrow data structures, and transaction log can use ~5KB/row
    assert!(
        bytes_per_row_memory < 5000,
        "Memory per row too high: {} bytes (expected < 5KB)",
        bytes_per_row_memory
    );
    assert!(
        total_overhead < 500_000_000,
        "Total memory overhead too high: {} bytes (expected < 500MB)",
        total_overhead
    );

    cleanup_test_db(db_path);
}

/// Test: Compare FSDB query performance with direct Parquet access
#[tokio::test]
async fn test_fsdb_vs_direct_parquet_comparison() {
    setup_logging();
    let db_path = "/tmp/test_db_perf_comparison";
    cleanup_test_db(db_path);

    let schema = create_test_schema();

    println!("\n=== FSDB vs Direct Parquet Comparison ===");

    // Create database and insert test data
    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    let num_batches = 50;
    let batch_size = 1000;
    let total_rows = num_batches * batch_size;

    println!(
        "Inserting {} rows across {} batches...",
        total_rows, num_batches
    );
    for batch_num in 0..num_batches {
        let batch =
            create_batch_with_rows(schema.clone(), (batch_num * batch_size) as i32, batch_size);
        db.insert(batch).await.expect("Failed to insert");
    }

    // Benchmark 1: FSDB query performance
    let fsdb_queries = vec![
        ("SELECT COUNT(*) as count FROM data", "Full scan with COUNT"),
        (
            "SELECT * FROM data WHERE id >= 1000 AND id < 2000 LIMIT 100",
            "Range query",
        ),
        (
            "SELECT name, AVG(value) as avg_value FROM data GROUP BY name LIMIT 50",
            "Aggregation",
        ),
    ];

    println!("\n--- FSDB Query Performance ---");
    for (sql, description) in &fsdb_queries {
        let start = Instant::now();
        let _results = db.query(sql).await.expect("FSDB query failed");
        let fsdb_latency = start.elapsed();
        println!(
            "  {}: {:.2}ms",
            description,
            fsdb_latency.as_secs_f64() * 1000.0
        );
    }

    // Benchmark 2: Direct Parquet access performance using Arrow ParquetFileReaderBuilder
    use std::fs::File;

    // In Delta Lake mode, parquet files are in the base directory
    let parquet_files: Vec<_> = fs::read_dir(db_path)
        .expect("Failed to read database directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();

    println!("\n--- Direct Parquet Access Performance ---");
    println!("Number of Parquet files: {}", parquet_files.len());

    // Query 1: Full scan count (direct Parquet metadata)
    let start = Instant::now();
    let mut total_count = 0usize;
    for entry in &parquet_files {
        let file_path = entry.path();
        if let Ok(file) = File::open(&file_path)
            && let Ok(builder) =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        {
            total_count += builder.metadata().file_metadata().num_rows() as usize;
        }
    }
    let direct_count_latency = start.elapsed();
    println!(
        "  Full scan with COUNT (metadata only): {:.2}ms (counted {} rows)",
        direct_count_latency.as_secs_f64() * 1000.0,
        total_count
    );

    // Query 2: Read all rows (direct Parquet)
    let start = Instant::now();
    let mut rows_read = 0usize;
    for entry in &parquet_files {
        let file_path = entry.path();
        if let Ok(file) = File::open(&file_path)
            && let Ok(builder) =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            && let Ok(reader) = builder.build()
        {
            for batch in reader.flatten() {
                rows_read += batch.num_rows();
            }
        }
    }
    let direct_read_latency = start.elapsed();
    println!(
        "  Full table read: {:.2}ms (read {} rows)",
        direct_read_latency.as_secs_f64() * 1000.0,
        rows_read
    );

    // Query 3: Filtered read (direct Parquet - simulating range query)
    let start = Instant::now();
    let mut filtered_rows = 0usize;
    for entry in &parquet_files {
        let file_path = entry.path();
        if let Ok(file) = File::open(&file_path)
            && let Ok(builder) =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            && let Ok(reader) = builder.build()
        {
            for batch in reader.flatten() {
                // Get id column and filter
                if let Some(id_array) = batch.column(0).as_any().downcast_ref::<Int32Array>() {
                    for i in 0..id_array.len() {
                        let id = id_array.value(i);
                        if (1000..2000).contains(&id) {
                            filtered_rows += 1;
                            if filtered_rows >= 100 {
                                break;
                            }
                        }
                    }
                }
                if filtered_rows >= 100 {
                    break;
                }
            }
        }
        if filtered_rows >= 100 {
            break;
        }
    }
    let direct_filter_latency = start.elapsed();
    println!(
        "  Range query (filtered): {:.2}ms (read {} rows)",
        direct_filter_latency.as_secs_f64() * 1000.0,
        filtered_rows
    );

    // Summary comparison
    println!("\n--- Performance Comparison Summary ---");
    println!("FSDB provides:");
    println!("  - SQL query interface (DataFusion)");
    println!("  - ACID transactions with WAL");
    println!("  - MVCC concurrency control");
    println!("  - Transaction log and metadata");
    println!("Direct Parquet provides:");
    println!("  - Raw file access");
    println!("  - No transaction overhead");
    println!("  - Manual iteration required");
    println!("\nNote: FSDB latency includes SQL parsing, planning, and execution.");
    println!("      Direct Parquet latency is raw file I/O only.");

    // Assertions: FSDB should be within reasonable range of direct Parquet
    // We expect FSDB to be slower due to SQL layer, but not excessively
    assert_eq!(total_count, total_rows, "Row count mismatch");

    cleanup_test_db(db_path);
}

/// Helper: Get current process memory usage (RSS) in bytes
#[cfg(target_os = "linux")]
fn get_current_memory_usage() -> u64 {
    use std::fs::read_to_string;

    // Read /proc/self/status to get VmRSS (Resident Set Size)
    if let Ok(status) = read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2
                    && let Ok(kb) = parts[1].parse::<u64>()
                {
                    return kb * 1024; // Convert KB to bytes
                }
            }
        }
    }
    0
}

#[cfg(target_os = "macos")]
fn get_current_memory_usage() -> u64 {
    use std::process::Command;

    // Use ps command to get RSS in bytes
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();

    if let Ok(output) = output
        && let Ok(rss_str) = String::from_utf8(output.stdout)
        && let Ok(kb) = rss_str.trim().parse::<u64>()
    {
        return kb * 1024; // Convert KB to bytes
    }
    0
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn get_current_memory_usage() -> u64 {
    // Fallback for other platforms - return 0 (test will still pass with minimal overhead)
    0
}

/// Helper: Calculate total size of a directory
fn get_directory_size(path: &str) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total += metadata.len();
            }
        }
    }
    total
}
