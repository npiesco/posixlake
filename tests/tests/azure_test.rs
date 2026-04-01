//! Integration tests for Azure Blob Storage / ADLS Gen2 backend
//!
//! These tests require Azurite to be running locally:
//! ```
//! docker-compose up -d azurite
//! ```
//!
//! Azurite configuration (well-known development credentials):
//! - Endpoint: http://127.0.0.1:10000
//! - Account Name: devstoreaccount1
//! - Account Key: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
//! - Container: posixlake-test

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use posixlake::DatabaseOps;
use std::env;
use std::sync::Arc;

/// Azurite well-known development credentials
const AZURITE_ACCOUNT_NAME: &str = "devstoreaccount1";
const AZURITE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Check if Azurite is available
async fn is_azurite_available() -> bool {
    if env::var("SKIP_AZURE_TESTS").is_ok() {
        return false;
    }

    let endpoint = get_azurite_endpoint();

    // Azurite blob service responds on the root endpoint
    match reqwest::get(format!(
        "{}/{}/posixlake-test?restype=container",
        endpoint, AZURITE_ACCOUNT_NAME
    ))
    .await
    {
        Ok(_) => true,
        Err(_) => {
            eprintln!("Azurite is not accessible at {}", endpoint);
            false
        }
    }
}

fn get_azurite_endpoint() -> String {
    env::var("AZURITE_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:10000".to_string())
}

/// Get Azure configuration from environment or use Azurite defaults
fn get_azure_config() -> (String, String, String, String) {
    let endpoint = get_azurite_endpoint();
    let account_name =
        env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or_else(|_| AZURITE_ACCOUNT_NAME.to_string());
    let account_key =
        env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap_or_else(|_| AZURITE_ACCOUNT_KEY.to_string());
    let container =
        env::var("AZURE_STORAGE_CONTAINER").unwrap_or_else(|_| "posixlake-test".to_string());

    (endpoint, account_name, account_key, container)
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
async fn test_azure_create_database() {
    if !is_azurite_available().await {
        eprintln!("Skipping Azure test: Azurite not available (set AZURITE_ENDPOINT to enable)");
        return;
    }

    let (endpoint, account_name, account_key, container) = get_azure_config();

    let azure_path = format!("az://{}/test_db_{}", container, uuid::Uuid::new_v4());

    println!("Creating database at Azure path: {}", azure_path);
    println!("Azurite endpoint: {}", endpoint);

    let schema = create_test_schema();

    let result = DatabaseOps::create_with_azure(
        &azure_path,
        schema.clone(),
        &account_name,
        &account_key,
        &endpoint,
    )
    .await;

    assert!(
        result.is_ok(),
        "Failed to create database on Azure: {:?}",
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
async fn test_azure_insert_and_query() {
    if !is_azurite_available().await {
        eprintln!("Skipping Azure test: Azurite not available");
        return;
    }

    let (endpoint, account_name, account_key, container) = get_azure_config();
    let azure_path = format!("az://{}/test_db_{}", container, uuid::Uuid::new_v4());

    let schema = create_test_schema();
    let db = DatabaseOps::create_with_azure(
        &azure_path,
        schema.clone(),
        &account_name,
        &account_key,
        &endpoint,
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
async fn test_azure_reopen_database() {
    if !is_azurite_available().await {
        eprintln!("Skipping Azure test: Azurite not available");
        return;
    }

    let (endpoint, account_name, account_key, container) = get_azure_config();
    let azure_path = format!("az://{}/test_db_{}", container, uuid::Uuid::new_v4());

    let schema = create_test_schema();

    // Create and insert data
    {
        let db = DatabaseOps::create_with_azure(
            &azure_path,
            schema.clone(),
            &account_name,
            &account_key,
            &endpoint,
        )
        .await
        .unwrap();

        let batch = create_test_batch(schema.clone(), 10);
        db.insert(batch).await.unwrap();
    }

    // Reopen database
    let db =
        DatabaseOps::open_with_azure(&azure_path, &account_name, &account_key, &endpoint).await;

    assert!(
        db.is_ok(),
        "Failed to reopen database from Azure: {:?}",
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
async fn test_azure_multiple_inserts() {
    if !is_azurite_available().await {
        eprintln!("Skipping Azure test: Azurite not available");
        return;
    }

    let (endpoint, account_name, account_key, container) = get_azure_config();
    let azure_path = format!("az://{}/test_db_{}", container, uuid::Uuid::new_v4());

    let schema = create_test_schema();
    let db = DatabaseOps::create_with_azure(
        &azure_path,
        schema.clone(),
        &account_name,
        &account_key,
        &endpoint,
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
