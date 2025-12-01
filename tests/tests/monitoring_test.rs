use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::fs;
use std::sync::Arc;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_dir_all(path);
}

/// Test: Database metrics tracking for queries, inserts, deletes
#[tokio::test]
async fn test_database_metrics_tracking() {
    setup_logging();
    let db_path = "/tmp/test_db_metrics";
    cleanup_test_db(db_path);

    println!("\n=== Test: Database Metrics Tracking ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Get initial metrics - should be all zeros
    let initial_metrics = db.get_metrics().await;
    assert_eq!(initial_metrics.total_queries, 0);
    assert_eq!(initial_metrics.total_inserts, 0);
    assert_eq!(initial_metrics.total_deletes, 0);
    assert_eq!(initial_metrics.total_transactions, 0);
    assert_eq!(initial_metrics.total_errors, 0);
    println!("✓ Initial metrics all zero");

    // Perform 3 inserts
    for i in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("name_{}", i)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![i * 100])) as ArrayRef,
            ],
        )
        .unwrap();
        db.insert(batch).await.expect("Insert should succeed");
    }

    let after_insert_metrics = db.get_metrics().await;
    assert_eq!(
        after_insert_metrics.total_inserts, 3,
        "Should have 3 inserts"
    );
    assert_eq!(
        after_insert_metrics.total_transactions, 3,
        "Should have 3 transactions"
    );
    println!(
        "✓ Insert metrics tracked: {} inserts, {} transactions",
        after_insert_metrics.total_inserts, after_insert_metrics.total_transactions
    );

    // Perform 5 queries
    for _ in 0..5 {
        db.query("SELECT * FROM data")
            .await
            .expect("Query should succeed");
    }

    let after_query_metrics = db.get_metrics().await;
    assert_eq!(
        after_query_metrics.total_queries, 5,
        "Should have 5 queries"
    );
    println!(
        "✓ Query metrics tracked: {} queries",
        after_query_metrics.total_queries
    );

    // Perform 1 delete (Delta Lake row-level deletion)
    db.delete_rows_where("id = 0")
        .await
        .expect("Delete should succeed");

    let final_metrics = db.get_metrics().await;
    assert_eq!(final_metrics.total_deletes, 1, "Should have 1 delete");
    assert_eq!(
        final_metrics.total_inserts, 3,
        "Insert count should remain 3"
    );
    assert_eq!(
        final_metrics.total_queries, 5,
        "Query count should remain 5"
    );
    println!(
        "✓ Delete metrics tracked: {} deletes",
        final_metrics.total_deletes
    );

    // Verify uptime is greater than 0
    assert!(
        final_metrics.uptime_seconds > 0.0,
        "Uptime should be tracked"
    );
    println!(
        "✓ Uptime tracked: {:.2} seconds",
        final_metrics.uptime_seconds
    );

    cleanup_test_db(db_path);
}

/// Test: Health check returns database status
#[tokio::test]
async fn test_health_check() {
    setup_logging();
    let db_path = "/tmp/test_db_health";
    cleanup_test_db(db_path);

    println!("\n=== Test: Health Check ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    let health = db.health_check().await;

    assert_eq!(health.status, "healthy", "Database should be healthy");
    assert!(health.uptime_seconds > 0.0, "Uptime should be positive");
    assert_eq!(health.total_files, 0, "Should start with 0 files");
    assert_eq!(health.total_rows, 0, "Should start with 0 rows");
    println!("✓ Health status: {}", health.status);
    println!("✓ Uptime: {:.2}s", health.uptime_seconds);

    // Insert data and check health again
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let health_after = db.health_check().await;
    // Note: Delta Lake doesn't expose file/row counts without scanning
    // Health check only verifies that _delta_log exists
    assert_eq!(
        health_after.status, "healthy",
        "Should be healthy after insert"
    );
    println!("✓ Health after insert: status={}", health_after.status);

    cleanup_test_db(db_path);
}

/// Test: Query latency tracking
#[tokio::test]
async fn test_query_latency_tracking() {
    setup_logging();
    let db_path = "/tmp/test_db_latency";
    cleanup_test_db(db_path);

    println!("\n=== Test: Query Latency Tracking ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Insert test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Execute queries and check latency tracking
    for _ in 0..10 {
        db.query("SELECT * FROM data").await.unwrap();
    }

    let metrics = db.get_metrics().await;
    assert_eq!(metrics.total_queries, 10, "Should have 10 queries");
    assert!(
        metrics.avg_query_latency_ms > 0.0,
        "Average query latency should be tracked"
    );
    assert!(
        metrics.max_query_latency_ms >= metrics.avg_query_latency_ms,
        "Max latency should be >= average latency"
    );

    println!("✓ Latency metrics:");
    println!("  Average: {:.2}ms", metrics.avg_query_latency_ms);
    println!("  Max: {:.2}ms", metrics.max_query_latency_ms);

    cleanup_test_db(db_path);
}

/// Test: Error tracking in metrics
#[tokio::test]
async fn test_error_tracking() {
    setup_logging();
    let db_path = "/tmp/test_db_errors";
    cleanup_test_db(db_path);

    println!("\n=== Test: Error Tracking ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Attempt an invalid query (should increment error count) - use malformed SQL
    let result = db.query("INVALID SQL SYNTAX HERE").await;
    assert!(result.is_err(), "Query should fail for invalid SQL");

    let metrics = db.get_metrics().await;
    assert_eq!(
        metrics.total_errors, 1,
        "Error count should be 1 after failed query"
    );
    println!("✓ Error tracked: {} total errors", metrics.total_errors);

    // Attempt another invalid query (should increment error count)
    let second_result = db.query("ANOTHER BAD QUERY WITH SYNTAX ERROR").await;
    assert!(second_result.is_err(), "Query should fail for invalid SQL");

    let metrics_after = db.get_metrics().await;
    assert_eq!(
        metrics_after.total_errors, 2,
        "Error count should be 2 after second error"
    );
    println!(
        "✓ Multiple errors tracked: {} total errors",
        metrics_after.total_errors
    );

    cleanup_test_db(db_path);
}

/// Test: Metrics reset functionality
#[tokio::test]
async fn test_metrics_reset() {
    setup_logging();
    let db_path = "/tmp/test_db_reset";
    cleanup_test_db(db_path);

    println!("\n=== Test: Metrics Reset ===");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let db = DatabaseOps::create(db_path, schema.clone())
        .await
        .expect("Failed to create database");

    // Perform operations
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    db.insert(batch).await.unwrap();
    db.query("SELECT * FROM data").await.unwrap();

    let before_reset = db.get_metrics().await;
    assert!(before_reset.total_inserts > 0);
    assert!(before_reset.total_queries > 0);

    // Reset metrics
    db.reset_metrics().await;

    let after_reset = db.get_metrics().await;
    assert_eq!(after_reset.total_queries, 0, "Queries should be reset to 0");
    assert_eq!(after_reset.total_inserts, 0, "Inserts should be reset to 0");
    assert_eq!(after_reset.total_deletes, 0, "Deletes should be reset to 0");
    assert_eq!(after_reset.total_errors, 0, "Errors should be reset to 0");
    // Note: uptime should NOT be reset
    assert!(
        after_reset.uptime_seconds > 0.0,
        "Uptime should continue running"
    );

    println!("✓ Metrics reset successfully (uptime preserved)");

    cleanup_test_db(db_path);
}
