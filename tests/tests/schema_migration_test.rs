use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use posixlake::DatabaseOps;
use posixlake::metadata::schema::{
    DataTypeRepr, Schema as PosixSchema, SchemaField, SchemaManager,
};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("posixlake=info")
        .try_init();
}

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_dir_all(path);
}

fn test_db_path(name: &str) -> String {
    std::env::temp_dir()
        .join(name)
        .to_string_lossy()
        .into_owned()
}

/// Test: Query across multiple schema versions with automatic unification
#[tokio::test]
async fn test_query_with_schema_evolution() {
    setup_logging();
    let db_path = test_db_path("test_db_schema_migration");
    cleanup_test_db(&db_path);

    println!("\n=== Test: Query with Schema Evolution ===");

    // Schema V1: id, name
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema_v1.clone())
        .await
        .expect("Failed to create database");

    // Insert data with schema V1
    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        ],
    )
    .expect("Failed to create batch v1");

    db.insert(batch_v1).await.expect("Failed to insert v1 data");
    println!("Inserted 2 rows with schema v1 (id, name)");

    // Schema V2: id, name, age (new field)
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true), // Nullable for backward compatibility
    ]));

    // Insert data with schema V2
    let batch_v2 = RecordBatch::try_new(
        schema_v2.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Charlie", "Diana"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(35), Some(28)])) as ArrayRef,
        ],
    )
    .expect("Failed to create batch v2");

    db.insert(batch_v2).await.expect("Failed to insert v2 data");
    println!("Inserted 2 rows with schema v2 (id, name, age)");

    // Query should automatically unify schemas
    // V1 files should have NULL for age column
    // V2 files should have actual age values
    let results = db
        .query("SELECT * FROM data ORDER BY id")
        .await
        .expect("Query should work with mixed schema versions");

    assert_eq!(results.len(), 1, "Should return results");
    let batch = &results[0];

    // Verify unified schema has all columns
    assert_eq!(
        batch.num_columns(),
        3,
        "Unified schema should have 3 columns"
    );
    assert_eq!(batch.num_rows(), 4, "Should have all 4 rows");

    // Verify columns exist
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "name");
    assert_eq!(batch.schema().field(2).name(), "age");

    // Verify data
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_array.value(0), 1);
    assert_eq!(id_array.value(1), 2);
    assert_eq!(id_array.value(2), 3);
    assert_eq!(id_array.value(3), 4);

    let name_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_array.value(0), "Alice");
    assert_eq!(name_array.value(1), "Bob");
    assert_eq!(name_array.value(2), "Charlie");
    assert_eq!(name_array.value(3), "Diana");

    // Age column: NULL for v1 rows, actual values for v2 rows
    let age_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(age_array.is_null(0), "Alice (v1) should have NULL age");
    assert!(age_array.is_null(1), "Bob (v1) should have NULL age");
    assert_eq!(age_array.value(2), 35, "Charlie (v2) should have age 35");
    assert_eq!(age_array.value(3), 28, "Diana (v2) should have age 28");

    println!("✓ Query successfully unified schemas - v1 rows have NULL for new fields");

    cleanup_test_db(&db_path);
}

/// Test: Query with column projection on evolved schema
#[tokio::test]
async fn test_query_projection_with_evolved_schema() {
    setup_logging();
    let db_path = test_db_path("test_db_schema_projection");
    cleanup_test_db(&db_path);

    println!("\n=== Test: Query Projection with Evolved Schema ===");

    // Insert with V1 schema
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema_v1.clone())
        .await
        .expect("Failed to create database");

    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch_v1).await.unwrap();

    // Insert with V2 schema (added department)
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, true),
    ]));

    let batch_v2 = RecordBatch::try_new(
        schema_v2.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Bob"])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Engineering")])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch_v2).await.unwrap();

    // Query with projection - select only old columns
    let results = db
        .query("SELECT id, name FROM data ORDER BY id")
        .await
        .expect("Projection query should work");

    assert_eq!(results[0].num_columns(), 2);
    assert_eq!(results[0].num_rows(), 2);

    // Query with projection - select new column (should have NULL for v1 rows)
    let results = db
        .query("SELECT id, department FROM data ORDER BY id")
        .await
        .expect("Query with new column should work");

    let dept_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(dept_array.is_null(0), "Alice should have NULL department");
    assert_eq!(
        dept_array.value(1),
        "Engineering",
        "Bob should have Engineering dept"
    );

    cleanup_test_db(&db_path);
}

// =============================================================================
// Track 1: Schema Primary Key Metadata
// =============================================================================

/// RED TEST 1a: Schema should support primary_key metadata
/// Fails because Schema has no primary_key field yet
#[test]
fn test_schema_primary_key_metadata() {
    // Schema with a primary key designated
    let schema = PosixSchema::new(vec![
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
    ])
    .with_primary_key("id");

    assert_eq!(schema.primary_key(), Some("id"));

    // Schema without primary key should return None
    let schema_no_pk = PosixSchema::new(vec![SchemaField {
        name: "id".to_string(),
        data_type: DataTypeRepr::Int32,
        nullable: false,
    }]);
    assert_eq!(schema_no_pk.primary_key(), None);
}

/// RED TEST 1a (continued): primary_key survives serde round-trip
#[test]
fn test_schema_primary_key_serde_roundtrip() {
    let schema = PosixSchema::new(vec![
        SchemaField {
            name: "user_id".to_string(),
            data_type: DataTypeRepr::Int64,
            nullable: false,
        },
        SchemaField {
            name: "email".to_string(),
            data_type: DataTypeRepr::Utf8,
            nullable: false,
        },
    ])
    .with_primary_key("user_id");

    let json = serde_json::to_string(&schema).unwrap();
    let deserialized: PosixSchema = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.primary_key(), Some("user_id"));
}

/// RED TEST 1a (continued): primary_key survives SchemaManager write/read
#[test]
fn test_schema_primary_key_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_dir = temp_dir.path().join("_metadata");

    let manager = SchemaManager::new(&metadata_dir).unwrap();

    let schema = PosixSchema::new(vec![
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
    ])
    .with_primary_key("id");

    manager.write_schema(&schema).unwrap();
    let read_back = manager.read_schema().unwrap().unwrap();
    assert_eq!(read_back.primary_key(), Some("id"));
}

/// RED TEST 1a (continued): legacy schemas without primary_key deserialize with None
#[test]
fn test_schema_primary_key_backward_compat() {
    // Simulate a legacy schema JSON (no primary_key field)
    let legacy_json =
        r#"{"version":1,"fields":[{"name":"id","data_type":"Int32","nullable":false}]}"#;
    let schema: PosixSchema = serde_json::from_str(legacy_json).unwrap();
    assert_eq!(schema.primary_key(), None);
}
#[tokio::test]
async fn test_schema_compatibility() {
    setup_logging();
    let db_path = test_db_path("test_db_schema_compat");
    cleanup_test_db(&db_path);

    println!("\n=== Test: Schema Compatibility ===");

    // V1: id, name
    let schema_v1: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema_v1.clone())
        .await
        .unwrap();

    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch_v1).await.unwrap();

    // V2: id, name, age
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));

    let batch_v2 = RecordBatch::try_new(
        schema_v2.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Charlie"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(35)])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch_v2).await.unwrap();

    // V3: id, name, age, department
    let schema_v3 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("department", DataType::Utf8, true),
    ]));

    let batch_v3 = RecordBatch::try_new(
        schema_v3.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Diana"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(28)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Sales")])) as ArrayRef,
        ],
    )
    .unwrap();
    db.insert(batch_v3).await.unwrap();

    // Query all columns - should unify to V3 schema with NULLs for missing fields
    let results = db
        .query("SELECT * FROM data ORDER BY id")
        .await
        .expect("Query across 3 schema versions should work");

    assert_eq!(
        results[0].num_columns(),
        4,
        "Should have 4 columns (unified V3 schema)"
    );
    assert_eq!(results[0].num_rows(), 4, "Should have 4 rows");

    // Verify NULL handling across versions
    let age_array = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(age_array.is_null(0), "Alice (V1) should have NULL age");
    assert!(age_array.is_null(1), "Bob (V1) should have NULL age");
    assert_eq!(age_array.value(2), 35, "Charlie (V2) should have age");
    assert_eq!(age_array.value(3), 28, "Diana (V3) should have age");

    let dept_array = results[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(dept_array.is_null(0), "Alice (V1) should have NULL dept");
    assert!(dept_array.is_null(1), "Bob (V1) should have NULL dept");
    assert!(dept_array.is_null(2), "Charlie (V2) should have NULL dept");
    assert_eq!(dept_array.value(3), "Sales", "Diana (V3) should have dept");

    println!("✓ Successfully queried across 3 schema versions with correct NULL handling");

    cleanup_test_db(&db_path);
}

/// RED TEST 1c: DatabaseOps should persist and recover primary_key
/// Fails because DatabaseOps has no primary_key support yet
#[tokio::test]
async fn test_create_db_with_primary_key() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_pk");

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create database and set primary key
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    db.set_primary_key("user_id").unwrap();
    assert_eq!(db.primary_key(), Some("user_id".to_string()));

    // Re-open and verify primary key persists
    drop(db);
    let db2 = DatabaseOps::open(&db_path).await.unwrap();
    assert_eq!(db2.primary_key(), Some("user_id".to_string()));
}

/// RED TEST 1c (continued): DatabaseOps without primary_key returns None
#[tokio::test]
async fn test_db_without_primary_key() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_no_pk");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    assert_eq!(db.primary_key(), None);
}
