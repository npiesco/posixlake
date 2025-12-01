//! Example 4: Schema Evolution
//!
//! This example demonstrates:
//! - Creating a database with an initial schema
//! - Evolving the schema by adding new fields
//! - Schema versioning and history tracking
//! - Querying with evolved schemas

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::metadata::schema::DataTypeRepr;
use fsdb::metadata::{Schema as FsdbSchema, SchemaField, SchemaManager};
use fsdb::DatabaseOps;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .init();

    println!("\n=== FSDB Schema Evolution Example ===\n");

    // 1. Create initial schema (version 1)
    println!("1. Creating database with initial schema (v1)...");
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db_path = "/tmp/fsdb_example_schema_evolution";
    let _ = std::fs::remove_dir_all(db_path);

    let db = DatabaseOps::create(db_path, schema_v1.clone()).await?;
    println!("   Schema v1: id (Int32), name (Utf8)");

    // 2. Insert data with schema v1
    println!("\n2. Inserting data with schema v1...");
    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        ],
    )?;
    db.insert(batch_v1).await?;
    println!("   Inserted 2 rows with schema v1");

    // 3. Demonstrate schema evolution concept
    println!("\n3. Demonstrating schema evolution with SchemaManager...");
    let schema_manager = SchemaManager::new(format!("{}/_metadata", db_path))?;

    // Save initial schema
    let mut fsdb_schema_v1 = FsdbSchema::new(vec![
        SchemaField {
            name: "id".to_string(),
            data_type: DataTypeRepr::Int32,
            nullable: false,
        },
        SchemaField {
            name: "name".to_string(),
            data_type: DataTypeRepr::Utf8,
            nullable: false,
        },
    ]);
    schema_manager.write_schema(&fsdb_schema_v1)?;
    println!("   Saved schema v1 to metadata");

    // 4. Evolve schema by adding a new field (version 2)
    println!("\n4. Evolving schema to v2 (adding 'age' field)...");
    fsdb_schema_v1.add_field(SchemaField {
        name: "age".to_string(),
        data_type: DataTypeRepr::Int32,
        nullable: true, // New fields should be nullable for compatibility
    });
    schema_manager.write_schema(&fsdb_schema_v1)?;
    println!("   Schema v2: id (Int32), name (Utf8), age (Int32, nullable)");

    // 5. Insert data with schema v2
    println!("\n5. Inserting data with evolved schema v2...");
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));

    let batch_v2 = RecordBatch::try_new(
        schema_v2.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Charlie", "Diana"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(35), Some(28)])) as ArrayRef,
        ],
    )?;
    db.insert(batch_v2).await?;
    println!("   Inserted 2 rows with schema v2 (including age)");

    // 6. Query data with automatic schema unification
    println!("\n6. Querying data with mixed schema versions...");
    println!("   FSDB automatically unifies schemas by adding NULL columns for missing fields");

    // Count all rows across both schema versions
    let count_result = db.query("SELECT COUNT(*) as count FROM data").await?;
    let count = count_result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("   ✓ Total rows across all schema versions: {}", count);

    // Query specific columns - old rows will have NULL for 'age'
    let results = db
        .query("SELECT id, name, age FROM data ORDER BY id")
        .await?;
    println!("\n   ✓ Queried all rows with unified schema:");
    println!("   Schema v1 rows (id=1,2) will have NULL age");
    println!("   Schema v2 rows (id=3,4) will have actual age values");

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
        let ages = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            let name = names.value(i);
            let age_str = if ages.is_null(i) {
                "NULL (from v1 schema)".to_string()
            } else {
                format!("{} (from v2 schema)", ages.value(i))
            };
            println!(
                "     Row {}: id={}, name={}, age={}",
                i + 1,
                id,
                name,
                age_str
            );
        }
    }

    // 7. Schema history
    println!("\n7. Schema versioning and history...");
    if let Some(current_schema) = schema_manager.read_schema()? {
        println!("   Current schema version: {}", current_schema.version);
        println!("   Fields: {}", current_schema.fields.len());
        for field in &current_schema.fields {
            println!(
                "     - {}: {:?} (nullable: {})",
                field.name, field.data_type, field.nullable
            );
        }
    } else {
        println!("   No current schema found");
    }

    let history = schema_manager.read_history()?;
    println!("\n   Schema history ({} versions):", history.len());
    for version in history {
        println!(
            "     v{}: {} fields",
            version.version,
            version.schema.fields.len()
        );
    }

    println!("\n=== Example Complete! ===\n");
    println!("Key takeaways:");
    println!("  ✓ Schemas can evolve by adding new fields using SchemaManager");
    println!("  ✓ New fields should be nullable for backward compatibility");
    println!("  ✓ Schema versioning tracks all historical changes");
    println!("  ✓ Old data files retain their original schema (append-only)");
    println!("  ✓ FSDB automatically unifies schemas when querying mixed versions");
    println!("  ✓ Missing fields are filled with NULL values during query execution");
    println!("  ✓ No manual intervention needed - schema evolution is fully automatic!");

    Ok(())
}
