use arrow::datatypes::{DataType, Field, Schema};
use fsdb::database_ops::DatabaseOps;
use fsdb::nfs::NfsServer;
use serial_test::serial;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Helper to create unique NFS server port based on thread ID
/// This allows parallel test execution without port conflicts
fn create_unique_port(base_port: u16) -> u16 {
    let thread_id = format!("{:?}", thread::current().id());
    let hash: u16 = thread_id.bytes().map(|b| b as u16).sum::<u16>() % 10000;
    base_port + hash
}

#[tokio::test]
#[serial] // NFS server tests must run serially to avoid port conflicts
async fn test_csv_generation_with_multiple_parquet_files() {
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

    // Now try to read via NFS server - use unique port to avoid conflicts
    let db_arc = Arc::new(db);
    let nfs_port = create_unique_port(12050);

    // Start NFS server
    let server = NfsServer::new(db_arc.clone(), nfs_port).await.unwrap();

    // Wait for server to be ready (event-based)
    assert!(server.is_ready().await, "NFS server should be ready");

    // Small delay for server to fully initialize
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Try to read the CSV file (read first 10KB)
    let csv_content = server.read_file("/data/data.csv", 0, 10240).await;

    match csv_content {
        Ok(content) => {
            let csv_str = String::from_utf8(content).unwrap();
            println!("CSV content:\n{}", csv_str);

            // Verify we got all 5 rows
            let lines: Vec<&str> = csv_str.trim().lines().collect();
            assert_eq!(lines.len(), 6, "Should have 1 header + 5 data rows");

            // Verify header
            assert!(lines[0].contains("id") && lines[0].contains("name"));
        }
        Err(e) => {
            panic!("Failed to read CSV: {}", e);
        }
    }

    // Cleanup: explicit shutdown to free the port
    server.shutdown().await.unwrap();
}
