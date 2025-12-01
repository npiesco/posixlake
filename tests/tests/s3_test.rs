//! Integration tests for S3/MinIO storage backend
//!
//! These tests require MinIO to be running locally:
//! ```
//! docker-compose up -d minio minio-init
//! ```
//!
//! MinIO configuration:
//! - Endpoint: http://localhost:9000
//! - Access Key: minioadmin
//! - Secret Key: minioadmin
//! - Bucket: fsdb-test

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fsdb::DatabaseOps;
use std::env;
use std::sync::Arc;

/// Helper to check if MinIO is available
async fn is_minio_available() -> bool {
    if env::var("SKIP_S3_TESTS").is_ok() {
        return false;
    }

    // Check if MinIO endpoint is actually accessible
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());

    // Try to make a simple HTTP request to MinIO health endpoint
    match reqwest::get(format!("{}/minio/health/live", endpoint)).await {
        Ok(resp) if resp.status().is_success() => true,
        _ => {
            eprintln!("MinIO is not accessible at {}", endpoint);
            false
        }
    }
}

/// Helper to get MinIO configuration from environment
fn get_minio_config() -> (String, String, String, String) {
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key = env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let bucket = env::var("MINIO_BUCKET").unwrap_or_else(|_| "fsdb-test".to_string());

    (endpoint, access_key, secret_key, bucket)
}

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]))
}

fn create_test_batch(schema: Arc<Schema>, start_id: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![start_id, start_id + 1, start_id + 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec![
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
            ])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_s3_create_database() {
    if !is_minio_available().await {
        eprintln!("Skipping S3 test: MinIO not available (set MINIO_ENDPOINT to enable)");
        return;
    }

    let (endpoint, access_key, secret_key, bucket) = get_minio_config();

    // S3 URI format: s3://bucket/prefix
    let s3_path = format!("s3://{}/test_db_{}", bucket, uuid::Uuid::new_v4());

    println!("Creating database at S3 path: {}", s3_path);
    println!("MinIO endpoint: {}", endpoint);

    let schema = create_test_schema();

    // This should create a database using S3 backend
    let result = DatabaseOps::create_with_s3(
        &s3_path,
        schema.clone(),
        &endpoint,
        &access_key,
        &secret_key,
    )
    .await;

    assert!(
        result.is_ok(),
        "Failed to create database on S3: {:?}",
        result.err()
    );
    let db = result.unwrap();

    // Verify schema
    let loaded_schema = db.schema();
    assert_eq!(loaded_schema.fields().len(), 3);
    assert_eq!(loaded_schema.field(0).name(), "id");
    assert_eq!(loaded_schema.field(1).name(), "name");
    assert_eq!(loaded_schema.field(2).name(), "email");
}

#[tokio::test]
async fn test_s3_insert_and_query() {
    if !is_minio_available().await {
        eprintln!("Skipping S3 test: MinIO not available");
        return;
    }

    let (endpoint, access_key, secret_key, bucket) = get_minio_config();
    let s3_path = format!("s3://{}/test_db_{}", bucket, uuid::Uuid::new_v4());

    let schema = create_test_schema();
    let db = DatabaseOps::create_with_s3(
        &s3_path,
        schema.clone(),
        &endpoint,
        &access_key,
        &secret_key,
    )
    .await
    .unwrap();

    // Insert data
    let batch = create_test_batch(schema.clone(), 1);
    let result = db.insert(batch).await;
    assert!(result.is_ok(), "Failed to insert data: {:?}", result.err());

    // Query data
    let query_result = db.query("SELECT * FROM data ORDER BY id").await;
    assert!(
        query_result.is_ok(),
        "Failed to query data: {:?}",
        query_result.err()
    );

    let batches = query_result.unwrap();
    assert_eq!(batches.len(), 1);

    let result_batch = &batches[0];
    assert_eq!(result_batch.num_rows(), 3);
    assert_eq!(result_batch.num_columns(), 3);

    // Verify data
    let id_col = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);
}

#[tokio::test]
async fn test_s3_reopen_database() {
    if !is_minio_available().await {
        eprintln!("Skipping S3 test: MinIO not available");
        return;
    }

    let (endpoint, access_key, secret_key, bucket) = get_minio_config();
    let s3_path = format!("s3://{}/test_db_{}", bucket, uuid::Uuid::new_v4());

    let schema = create_test_schema();

    // Create and insert data
    {
        let db = DatabaseOps::create_with_s3(
            &s3_path,
            schema.clone(),
            &endpoint,
            &access_key,
            &secret_key,
        )
        .await
        .unwrap();

        let batch = create_test_batch(schema.clone(), 10);
        db.insert(batch).await.unwrap();
    }

    // Reopen database
    let db = DatabaseOps::open_with_s3(&s3_path, &endpoint, &access_key, &secret_key).await;

    assert!(
        db.is_ok(),
        "Failed to reopen database from S3: {:?}",
        db.err()
    );
    let db = db.unwrap();

    // Verify data persisted
    let batches = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);

    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_col.value(0), 3, "Expected 3 rows to be persisted");

    // Verify schema
    let loaded_schema = db.schema();
    assert_eq!(loaded_schema.fields().len(), 3);
}

#[tokio::test]
async fn test_s3_multiple_inserts() {
    if !is_minio_available().await {
        eprintln!("Skipping S3 test: MinIO not available");
        return;
    }

    let (endpoint, access_key, secret_key, bucket) = get_minio_config();
    let s3_path = format!("s3://{}/test_db_{}", bucket, uuid::Uuid::new_v4());

    let schema = create_test_schema();
    let db = DatabaseOps::create_with_s3(
        &s3_path,
        schema.clone(),
        &endpoint,
        &access_key,
        &secret_key,
    )
    .await
    .unwrap();

    // Insert multiple batches
    for i in 0..3 {
        let batch = create_test_batch(schema.clone(), i * 10);
        db.insert(batch).await.unwrap();
    }

    // Verify all data
    let batches = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count_col.value(0),
        9,
        "Expected 9 rows total (3 batches * 3 rows)"
    );
}

use arrow::array::Int64Array;
