/// Example: Delta Lake Native Integration
///
/// This example demonstrates FSDB's native Delta Lake integration.
/// All data is stored in Delta Lake format (_delta_log) making it
/// readable by Spark, Databricks, and other Delta Lake tools.
///
/// Features demonstrated:
/// - Create database in Delta Lake format
/// - Insert data (writes to Delta Lake)
/// - Query data (reads from Delta Lake)
/// - Row-level deletion with deletion vectors
/// - Open existing Delta Lake tables
/// - Transaction support
/// - Schema evolution
use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fsdb::database_ops::DatabaseOps;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== FSDB Delta Lake Native Integration Example ===\n");

    // Create a temporary directory for this example
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delta_lake_db");

    // ========================================
    // Step 1: Create Database in Delta Lake Format
    // ========================================
    println!("Step 1: Creating database in Delta Lake native format...");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let db = DatabaseOps::create_with_delta_native(db_path.clone(), schema.clone()).await?;
    println!("  ✓ Database created at: {}", db_path.display());
    println!(
        "  ✓ Delta Lake _delta_log directory exists: {}",
        db_path.join("_delta_log").exists()
    );
    println!();

    // ========================================
    // Step 2: Insert Data (Writes to Delta Lake)
    // ========================================
    println!("Step 2: Inserting employee data...");

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Sales",
                "Engineering",
            ])),
            Arc::new(Float64Array::from(vec![120000.0, 85000.0, 95000.0])),
        ],
    )?;

    db.insert(batch1).await?;
    println!("  ✓ Inserted 3 employees");

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["David", "Eve"])),
            Arc::new(StringArray::from(vec!["Sales", "Engineering"])),
            Arc::new(Float64Array::from(vec![90000.0, 110000.0])),
        ],
    )?;

    db.insert(batch2).await?;
    println!("  ✓ Inserted 2 more employees");
    println!("  ✓ Total: 5 employees in Delta Lake table");
    println!();

    // ========================================
    // Step 3: Query Data (Reads from Delta Lake)
    // ========================================
    println!("Step 3: Querying Delta Lake table...");

    let results = db.query("SELECT * FROM data ORDER BY id").await?;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    println!("  ✓ Query returned {} rows", total_rows);

    // Show data
    println!("\nAll employees:");
    for batch in &results {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let depts = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let salaries = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            println!(
                "  ID: {}, Name: {}, Department: {}, Salary: ${:.2}",
                ids.value(i),
                names.value(i),
                depts.value(i),
                salaries.value(i)
            );
        }
    }
    println!();

    // Advanced queries
    println!("Engineering employees:");
    let eng_results = db
        .query(
            "SELECT name, salary FROM data WHERE department = 'Engineering' ORDER BY salary DESC",
        )
        .await?;
    for batch in &eng_results {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let salaries = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            println!("  {} - ${:.2}", names.value(i), salaries.value(i));
        }
    }
    println!();

    // ========================================
    // Step 4: Row-Level Deletion (Deletion Vectors)
    // ========================================
    println!("Step 4: Deleting rows with deletion vectors...");

    // Delete employees in Sales department
    db.delete_rows_where("department = 'Sales'").await?;
    println!("  ✓ Deleted employees in Sales department");

    // Query after deletion
    let after_delete = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = after_delete[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("  ✓ Remaining employees: {}", count);
    println!("  (Delta Lake deletion vectors hide deleted rows without rewriting files)");
    println!();

    // ========================================
    // Step 5: Schema Evolution
    // ========================================
    println!("Step 5: Schema evolution...");

    // Add a new field "hire_date" (will be NULL for existing rows)
    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("hire_date", DataType::Utf8, true), // New field!
    ]));

    let batch3 = RecordBatch::try_new(
        evolved_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![6])),
            Arc::new(StringArray::from(vec!["Frank"])),
            Arc::new(StringArray::from(vec!["Engineering"])),
            Arc::new(Float64Array::from(vec![105000.0])),
            Arc::new(StringArray::from(vec!["2025-01-15"])),
        ],
    )?;

    db.insert(batch3).await?;
    println!("  ✓ Inserted employee with new 'hire_date' field");
    println!("  ✓ Schema evolved automatically");

    // Query with evolved schema
    let evolved_results = db
        .query("SELECT name, hire_date FROM data WHERE hire_date IS NOT NULL")
        .await?;
    println!("Employees with hire dates:");
    for batch in &evolved_results {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if dates.is_valid(i) {
                println!("  {} hired on {}", names.value(i), dates.value(i));
            }
        }
    }
    println!();

    // ========================================
    // Step 6: Transactions
    // ========================================
    println!("Step 6: Explicit transactions...");

    let db = Arc::new(db);
    let txn = db.begin_transaction().await?;

    // Insert within transaction
    let batch4 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["Grace"])),
            Arc::new(StringArray::from(vec!["Engineering"])),
            Arc::new(Float64Array::from(vec![115000.0])),
        ],
    )?;

    txn.insert(batch4).await?;
    println!("  ✓ Inserted employee in transaction");

    // Query within transaction (read-your-own-writes)
    let txn_results = txn.query("SELECT COUNT(*) as count FROM data").await?;
    let txn_count = txn_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("  ✓ Count within transaction: {}", txn_count);

    // Commit transaction
    txn.commit().await?;
    println!("  ✓ Transaction committed to Delta Lake");
    println!();

    // ========================================
    // Step 7: Reopen as Existing Delta Lake Table
    // ========================================
    println!("Step 7: Reopening existing Delta Lake table...");

    // Drop current handle and reopen
    drop(db);

    let db_reopened = DatabaseOps::open_delta_native(db_path.clone()).await?;
    println!("  ✓ Reopened Delta Lake table");

    let final_results = db_reopened
        .query("SELECT COUNT(*) as count FROM data")
        .await?;
    let final_count = final_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("  ✓ Total employees after reopen: {}", final_count);
    println!();

    // ========================================
    // Step 8: Delta Lake Compatibility
    // ========================================
    println!("Step 8: Delta Lake compatibility check...");

    // Verify Delta Lake directory structure
    let delta_log_dir = db_path.join("_delta_log");
    println!(
        "  ✓ Delta Lake _delta_log directory: {}",
        delta_log_dir.exists()
    );

    // List Delta Lake transaction log files
    let mut log_files = std::fs::read_dir(&delta_log_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|e| e == "json")
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    log_files.sort_by_key(|e| e.path());

    println!("  ✓ Delta Lake transaction log files:");
    for entry in log_files.iter().take(5) {
        println!("  - {}", entry.file_name().to_string_lossy());
    }
    if log_files.len() > 5 {
        println!("  ... and {} more", log_files.len() - 5);
    }
    println!();

    println!("Delta Lake Native Integration Complete!");
    println!("\nKey Features Demonstrated:");
    println!("  ✓ Native Delta Lake format (_delta_log)");
    println!("  ✓ Write via DeltaOps::write()");
    println!("  ✓ Query via Delta Lake snapshots");
    println!("  ✓ Row-level deletion with deletion vectors");
    println!("  ✓ Schema evolution");
    println!("  ✓ ACID transactions");
    println!("  ✓ Spark/Databricks compatible");
    println!("\nThis database is now readable by:");
    println!("   - Apache Spark with Delta Lake");
    println!("   - Databricks");
    println!("   - delta-rs (Rust)");
    println!("   - deltalake-python");
    println!("   - Any Delta Lake compatible tool");

    Ok(())
}
