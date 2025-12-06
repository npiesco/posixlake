//! Integration tests for complex data types: Decimal, List, Map, Struct
//!
//! Tests that posixlake properly handles complex Arrow data types through
//! the full pipeline: create, insert, query, and schema retrieval.

use arrow::array::{
    ArrayRef, Decimal128Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a database with a given schema
async fn create_db_with_schema(
    path: &str,
    schema: Arc<Schema>,
) -> posixlake::database_ops::DatabaseOps {
    posixlake::database_ops::DatabaseOps::create(path, schema)
        .await
        .expect("Failed to create database")
}

#[tokio::test]
async fn test_decimal128_type() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("decimal_db");

    // Create schema with Decimal128(10, 2) - 10 digits, 2 decimal places
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("price", DataType::Decimal128(10, 2), false),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Insert data with decimal values (stored as i128, scaled by 10^2)
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let price_array = Decimal128Array::from(vec![1999i128, 4999i128, 9999i128]) // $19.99, $49.99, $99.99
        .with_precision_and_scale(10, 2)
        .unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(price_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    // Verify schema preserved
    let result_schema = results[0].schema();
    assert!(matches!(
        result_schema.field(1).data_type(),
        DataType::Decimal128(10, 2)
    ));

    println!("✓ test_decimal128_type PASSED");
}

#[tokio::test]
async fn test_list_type() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("list_db");

    // Create schema with List<Int32>
    let list_field = Field::new("item", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("scores", DataType::List(Arc::new(list_field)), true),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Create list data
    let id_array = Int32Array::from(vec![1, 2]);

    // Build list array: [[10, 20, 30], [40, 50]]
    let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
    let offsets = OffsetBuffer::new(vec![0i32, 3, 5].into());
    let list_array = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        offsets,
        Arc::new(values),
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(list_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify schema preserved
    let result_schema = results[0].schema();
    assert!(matches!(
        result_schema.field(1).data_type(),
        DataType::List(_)
    ));

    println!("✓ test_list_type PASSED");
}

#[tokio::test]
async fn test_struct_type() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("struct_db");

    // Create schema with Struct<name:String, age:Int32>
    let struct_fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "person",
            DataType::Struct(struct_fields.clone().into()),
            true,
        ),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Create struct data
    let id_array = Int32Array::from(vec![1, 2]);

    let name_array = StringArray::from(vec!["Alice", "Bob"]);
    let age_array = Int32Array::from(vec![30, 25]);

    let struct_array = StructArray::new(
        struct_fields.into(),
        vec![
            Arc::new(name_array) as ArrayRef,
            Arc::new(age_array) as ArrayRef,
        ],
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(struct_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify schema preserved
    let result_schema = results[0].schema();
    assert!(matches!(
        result_schema.field(1).data_type(),
        DataType::Struct(_)
    ));

    println!("✓ test_struct_type PASSED");
}

#[tokio::test]
async fn test_map_type() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("map_db");

    // Create schema with Map<String, Int32>
    let map_field = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ]
            .into(),
        ),
        false,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "attributes",
            DataType::Map(Arc::new(map_field), false),
            true,
        ),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Create map data - two rows with key-value pairs
    let id_array = Int32Array::from(vec![1, 2]);

    // Build map: row1: {"a": 1, "b": 2}, row2: {"c": 3}
    let keys = StringArray::from(vec!["a", "b", "c"]);
    let values = Int32Array::from(vec![1, 2, 3]);

    let struct_array = StructArray::new(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]
        .into(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    );

    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let map_array = MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        offsets,
        struct_array,
        None,
        false,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(map_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify schema preserved
    let result_schema = results[0].schema();
    assert!(matches!(
        result_schema.field(1).data_type(),
        DataType::Map(_, _)
    ));

    println!("✓ test_map_type PASSED");
}

#[tokio::test]
async fn test_nested_list_of_structs() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("nested_db");

    // Create schema with List<Struct<x:Int32, y:Int32>> (e.g., list of points)
    let point_fields = vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
    ];
    let point_struct = DataType::Struct(point_fields.clone().into());
    let list_field = Field::new("item", point_struct.clone(), true);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("points", DataType::List(Arc::new(list_field)), true),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Create nested data: row1 has 2 points, row2 has 1 point
    let id_array = Int32Array::from(vec![1, 2]);

    // Points: (0,0), (1,1), (2,2)
    let x_array = Int32Array::from(vec![0, 1, 2]);
    let y_array = Int32Array::from(vec![0, 1, 2]);

    let struct_array = StructArray::new(
        point_fields.into(),
        vec![Arc::new(x_array) as ArrayRef, Arc::new(y_array) as ArrayRef],
        None,
    );

    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let list_array = ListArray::new(
        Arc::new(Field::new("item", point_struct, true)),
        offsets,
        Arc::new(struct_array),
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(list_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    println!("✓ test_nested_list_of_structs PASSED");
}

/// Note: Decimal256 is not supported by Delta Lake protocol.
/// Delta Lake only supports Decimal128 (up to 38 digits precision).
/// This test verifies that Decimal128 with max precision works.
#[tokio::test]
async fn test_decimal128_max_precision() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("decimal_max_db");

    // Create schema with Decimal128(38, 10) - max precision
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("big_number", DataType::Decimal128(38, 10), false),
    ]));

    let db = create_db_with_schema(db_path.to_str().unwrap(), schema.clone()).await;

    // Insert data with large decimal values
    let id_array = Int32Array::from(vec![1, 2]);
    let big_number_array =
        Decimal128Array::from(vec![12345678901234567890i128, 98765432109876543210i128])
            .with_precision_and_scale(38, 10)
            .unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(big_number_array)],
    )
    .unwrap();

    db.insert(batch).await.expect("Insert failed");

    // Query and verify
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify schema preserved
    let result_schema = results[0].schema();
    assert!(matches!(
        result_schema.field(1).data_type(),
        DataType::Decimal128(38, 10)
    ));

    println!("✓ test_decimal128_max_precision PASSED");
}
