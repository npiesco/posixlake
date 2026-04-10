//! Integration tests for Microsoft Fabric OneLake storage backend
//!
//! These tests require:
//! 1. A Fabric workspace with a lakehouse (see FABRIC_SETUP.md)
//! 2. A Service Principal with Contributor access on the workspace
//! 3. Environment variables set (see below)
//!
//! Environment variables:
//! - FABRIC_ONELAKE_TABLES_PATH: OneLake ABFSS tables path
//!   e.g. abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables
//! - AZURE_STORAGE_CLIENT_ID: Service Principal app ID
//! - AZURE_STORAGE_CLIENT_SECRET: Service Principal password
//! - AZURE_STORAGE_TENANT_ID: Entra tenant ID

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use posixlake::DatabaseOps;
use std::env;
use std::sync::Arc;

/// Check if Fabric/OneLake credentials are available
fn has_fabric_credentials() -> bool {
    env::var("FABRIC_ONELAKE_TABLES_PATH").is_ok()
        && env::var("AZURE_STORAGE_CLIENT_ID").is_ok()
        && env::var("AZURE_STORAGE_CLIENT_SECRET").is_ok()
        && env::var("AZURE_STORAGE_TENANT_ID").is_ok()
}

fn get_fabric_config() -> (String, String, String, String) {
    let tables_path =
        env::var("FABRIC_ONELAKE_TABLES_PATH").expect("FABRIC_ONELAKE_TABLES_PATH must be set");
    let client_id =
        env::var("AZURE_STORAGE_CLIENT_ID").expect("AZURE_STORAGE_CLIENT_ID must be set");
    let client_secret =
        env::var("AZURE_STORAGE_CLIENT_SECRET").expect("AZURE_STORAGE_CLIENT_SECRET must be set");
    let tenant_id =
        env::var("AZURE_STORAGE_TENANT_ID").expect("AZURE_STORAGE_TENANT_ID must be set");

    (tables_path, client_id, client_secret, tenant_id)
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
async fn test_fabric_create_database() {
    if !has_fabric_credentials() {
        eprintln!(
            "Skipping Fabric test: set FABRIC_ONELAKE_TABLES_PATH, AZURE_STORAGE_CLIENT_ID, AZURE_STORAGE_CLIENT_SECRET, AZURE_STORAGE_TENANT_ID"
        );
        return;
    }

    let (tables_path, client_id, client_secret, tenant_id) = get_fabric_config();
    let table_name = format!("posixlake_test_{}", uuid::Uuid::new_v4().simple());
    let onelake_path = format!("{}/{}", tables_path, table_name);

    println!("Creating database at OneLake path: {}", onelake_path);

    let schema = create_test_schema();

    let result = DatabaseOps::create_with_onelake(
        &onelake_path,
        schema.clone(),
        &client_id,
        &client_secret,
        &tenant_id,
    )
    .await;

    assert!(
        result.is_ok(),
        "Failed to create database on Fabric OneLake: {:?}",
        result.err()
    );
    let db = result.unwrap();

    let loaded_schema = db.schema();
    assert_eq!(loaded_schema.fields().len(), 3);
    assert_eq!(loaded_schema.field(0).name(), "id");
    assert_eq!(loaded_schema.field(1).name(), "name");
    assert_eq!(loaded_schema.field(2).name(), "email");
}

#[tokio::test]
async fn test_fabric_insert_and_query() {
    if !has_fabric_credentials() {
        eprintln!("Skipping Fabric test: credentials not set");
        return;
    }

    let (tables_path, client_id, client_secret, tenant_id) = get_fabric_config();
    let table_name = format!("posixlake_test_{}", uuid::Uuid::new_v4().simple());
    let onelake_path = format!("{}/{}", tables_path, table_name);

    let schema = create_test_schema();
    let db = DatabaseOps::create_with_onelake(
        &onelake_path,
        schema.clone(),
        &client_id,
        &client_secret,
        &tenant_id,
    )
    .await
    .unwrap();

    let batch = create_test_batch(schema.clone(), 1);
    let result = db.insert(batch).await;
    assert!(result.is_ok(), "Failed to insert data: {:?}", result.err());

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
async fn test_fabric_reopen_database() {
    if !has_fabric_credentials() {
        eprintln!("Skipping Fabric test: credentials not set");
        return;
    }

    let (tables_path, client_id, client_secret, tenant_id) = get_fabric_config();
    let table_name = format!("posixlake_test_{}", uuid::Uuid::new_v4().simple());
    let onelake_path = format!("{}/{}", tables_path, table_name);

    let schema = create_test_schema();

    {
        let db = DatabaseOps::create_with_onelake(
            &onelake_path,
            schema.clone(),
            &client_id,
            &client_secret,
            &tenant_id,
        )
        .await
        .unwrap();

        let batch = create_test_batch(schema.clone(), 10);
        db.insert(batch).await.unwrap();
    }

    let db =
        DatabaseOps::open_with_onelake(&onelake_path, &client_id, &client_secret, &tenant_id).await;

    assert!(
        db.is_ok(),
        "Failed to reopen database from Fabric: {:?}",
        db.err()
    );
    let db = db.unwrap();

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
}
