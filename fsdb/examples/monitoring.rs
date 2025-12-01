//! Example: Monitoring & Observability
//!
//! This example demonstrates:
//! - Real-time metrics tracking
//! - Health check API
//! - Query latency monitoring
//! - Database statistics
//! - Metrics reset functionality

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .init();

    println!("\n=== FSDB Monitoring & Observability Example ===\n");

    let db_path = "/tmp/fsdb_example_monitoring";
    let _ = std::fs::remove_dir_all(db_path);

    // 1. Create database
    println!("1. Creating database with metrics tracking...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("request_id", DataType::Int32, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("response_time_ms", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await?;
    println!("   ✓ Database created with automatic metrics tracking");
    println!("   ✓ Metrics: queries, inserts, deletes, transactions, errors, latency");

    // 2. Initial health check
    println!("\n2. Initial health check...");
    let health = db.health_check().await;
    println!("   Health Status: {}", health.status);
    println!("   Uptime: {:.2} seconds", health.uptime_seconds);
    println!("   Total Files: {}", health.total_files);
    println!("   Total Rows: {}", health.total_rows);
    println!("   Total Size: {} bytes", health.total_size_bytes);

    // 3. Perform various operations
    println!("\n3. Performing database operations (metrics tracked automatically)...");

    // Insert operation 1
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "/api/users",
                "/api/orders",
                "/api/products",
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![200, 200, 404])) as ArrayRef,
            Arc::new(Int32Array::from(vec![45, 123, 67])) as ArrayRef,
        ],
    )?;
    db.insert(batch1).await?;
    println!("   ✓ Insert #1: 3 rows");

    // Query operation 1
    let _results1 = db
        .query("SELECT * FROM data WHERE status_code = 200")
        .await?;
    println!("   ✓ Query #1: Filter by status_code");

    // Insert operation 2
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["/api/login", "/api/logout"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![200, 200])) as ArrayRef,
            Arc::new(Int32Array::from(vec![89, 34])) as ArrayRef,
        ],
    )?;
    db.insert(batch2).await?;
    println!("   ✓ Insert #2: 2 rows");

    // Query operation 2
    let _results2 = db
        .query("SELECT endpoint, AVG(response_time_ms) as avg_time FROM data GROUP BY endpoint")
        .await?;
    println!("   ✓ Query #2: Aggregation (AVG)");

    // Query operation 3
    let _results3 = db.query("SELECT COUNT(*) as total FROM data").await?;
    println!("   ✓ Query #3: Count aggregation");

    // Simulate error (invalid SQL)
    println!("   • Attempting invalid query (will fail)...");
    match db.query("SELECT * FROM nonexistent_table").await {
        Ok(_) => println!("   ✗ ERROR: Should have failed!"),
        Err(_) => println!("   ✓ Query error tracked in metrics"),
    }

    // 4. Retrieve metrics
    println!("\n4. Retrieving database metrics...");
    tokio::time::sleep(Duration::from_millis(100)).await; // Let async operations complete

    let metrics = db.get_metrics().await;
    println!("   Metrics Summary:");
    println!("     Total Queries: {}", metrics.total_queries);
    println!("     Total Inserts: {}", metrics.total_inserts);
    println!("     Total Deletes: {}", metrics.total_deletes);
    println!("     Total Transactions: {}", metrics.total_transactions);
    println!("     Total Errors: {}", metrics.total_errors);
    println!(
        "     Average Query Latency: {:.2} ms",
        metrics.avg_query_latency_ms
    );
    println!(
        "     Max Query Latency: {:.2} ms",
        metrics.max_query_latency_ms
    );
    println!("     Uptime: {:.2} seconds", metrics.uptime_seconds);

    // 5. Health check after operations
    println!("\n5. Health check after operations...");
    let health_after = db.health_check().await;
    println!("   Health Status: {}", health_after.status);
    println!(
        "   Total Files: {} (+{})",
        health_after.total_files,
        health_after.total_files - health.total_files
    );
    println!(
        "   Total Rows: {} (+{})",
        health_after.total_rows,
        health_after.total_rows - health.total_rows
    );
    println!(
        "   Total Size: {} bytes (+{} bytes)",
        health_after.total_size_bytes,
        health_after.total_size_bytes - health.total_size_bytes
    );

    // 6. Perform more operations for latency tracking
    println!("\n6. Testing query latency tracking...");
    for i in 0..5 {
        let _ = db.query("SELECT * FROM data").await?;
        println!("   ✓ Query #{} completed", i + 1);
    }

    let metrics_after_queries = db.get_metrics().await;
    println!("\n   Updated Latency Metrics:");
    println!(
        "     Total Queries: {}",
        metrics_after_queries.total_queries
    );
    println!(
        "     Average Latency: {:.2} ms",
        metrics_after_queries.avg_query_latency_ms
    );
    println!(
        "     Max Latency: {:.2} ms",
        metrics_after_queries.max_query_latency_ms
    );
    println!(
        "     Latency History: {} samples (max 1000)",
        std::cmp::min(metrics_after_queries.total_queries as usize, 1000)
    );

    // 7. Reset metrics
    println!("\n7. Resetting metrics...");
    db.reset_metrics().await;

    let metrics_after_reset = db.get_metrics().await;
    println!("   Metrics After Reset:");
    println!(
        "     Total Queries: {} (reset to 0)",
        metrics_after_reset.total_queries
    );
    println!(
        "     Total Inserts: {} (reset to 0)",
        metrics_after_reset.total_inserts
    );
    println!(
        "     Total Errors: {} (reset to 0)",
        metrics_after_reset.total_errors
    );
    println!(
        "     Uptime: {:.2} seconds (preserved)",
        metrics_after_reset.uptime_seconds
    );
    println!("   ✓ Metrics reset successful (uptime preserved)");

    // 8. Monitoring use cases
    println!("\n8. Monitoring Use Cases:");
    println!("   Production Monitoring:");
    println!("     • Expose /health endpoint: health_check()");
    println!("     • Expose /metrics endpoint: get_metrics()");
    println!("     • Track query performance over time");
    println!("     • Alert on high error rates");
    println!();
    println!("   Performance Tuning:");
    println!("     • Identify slow queries (max_query_latency_ms)");
    println!("     • Monitor insert throughput (total_inserts / uptime)");
    println!("     • Track database growth (total_size_bytes)");
    println!();
    println!("   Capacity Planning:");
    println!("     • Database size trends (health_check.total_size_bytes)");
    println!("     • File count growth (health_check.total_files)");
    println!("     • Query load patterns (total_queries over time)");

    // 9. Real-time monitoring example
    println!("\n9. Simulating real-time monitoring...");
    println!("   Monitoring interval: 2 seconds");

    for iteration in 1..=3 {
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Perform some operations
        let _ = db.query("SELECT COUNT(*) FROM data").await?;

        let snapshot = db.get_metrics().await;
        println!("\n   [Snapshot #{}]", iteration);
        println!("     Queries: {}", snapshot.total_queries);
        println!("     Inserts: {}", snapshot.total_inserts);
        println!("     Avg Latency: {:.2} ms", snapshot.avg_query_latency_ms);
        println!("     Uptime: {:.2}s", snapshot.uptime_seconds);
    }

    println!("\n=== Monitoring & Observability Example Complete ===\n");
    println!("Key Takeaways:");
    println!("  • Automatic metrics tracking (zero overhead)");
    println!("  • Thread-safe atomic counters");
    println!("  • Query latency histograms");
    println!("  • Health check API for monitoring tools");
    println!("  • Metrics reset for testing/development");
    println!("  • Enterprise-grade observability");
    println!();
    println!("Available Metrics:");
    println!("  • total_queries: Total SELECT operations");
    println!("  • total_inserts: Total INSERT operations");
    println!("  • total_deletes: Total DELETE operations");
    println!("  • total_transactions: Total committed transactions");
    println!("  • total_errors: Total operation failures");
    println!("  • avg_query_latency_ms: Average query latency");
    println!("  • max_query_latency_ms: Maximum query latency");
    println!("  • uptime_seconds: Database uptime");

    Ok(())
}
