use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use posixlake::DatabaseOps;
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(schema: &Arc<Schema>, ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write_legacy_schema_metadata(db_path: &Path) {
    let metadata_dir = db_path.join("_metadata");
    fs::create_dir_all(&metadata_dir).unwrap();
    fs::write(
        metadata_dir.join("schema.json"),
        r#"{
  "version": 1,
  "fields": [
    { "name": "id", "data_type": "Int32", "nullable": false },
    { "name": "name", "data_type": "Utf8", "nullable": false }
  ]
}"#,
    )
    .unwrap();

    let history_path = metadata_dir.join("schema_history.json");
    if history_path.exists() {
        fs::remove_file(history_path).unwrap();
    }
}

async fn create_legacy_style_database(db_path: &Path) {
    let schema = test_schema();
    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();
    db.insert(make_batch(&schema, vec![1, 2], vec!["alice", "bob"]))
        .await
        .unwrap();
    write_legacy_schema_metadata(db_path);
}

async fn count_rows(db: &DatabaseOps) -> i64 {
    let results = db
        .query("SELECT COUNT(*) AS count FROM data")
        .await
        .unwrap();
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    count_array.value(0)
}

#[tokio::test]
async fn test_open_legacy_metadata_database_and_preserve_rows() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("legacy-db");

    create_legacy_style_database(&db_path).await;

    let db = DatabaseOps::open(&db_path).await.unwrap();
    assert_eq!(db.primary_key(), None);
    assert_eq!(count_rows(&db).await, 2);

    let schema_json = fs::read_to_string(db_path.join("_metadata").join("schema.json")).unwrap();
    let parsed: Value = serde_json::from_str(&schema_json).unwrap();
    assert!(parsed.get("primary_key").is_none());
}

#[tokio::test]
async fn test_upgrade_legacy_metadata_writes_current_fields_without_data_loss() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("legacy-upgrade-db");

    create_legacy_style_database(&db_path).await;

    let db = DatabaseOps::open(&db_path).await.unwrap();
    db.set_primary_key("id").unwrap();
    assert_eq!(db.primary_key().as_deref(), Some("id"));
    assert_eq!(count_rows(&db).await, 2);
    drop(db);

    let reopened = DatabaseOps::open(&db_path).await.unwrap();
    assert_eq!(reopened.primary_key().as_deref(), Some("id"));
    assert_eq!(count_rows(&reopened).await, 2);

    let schema_json = fs::read_to_string(db_path.join("_metadata").join("schema.json")).unwrap();
    let parsed: Value = serde_json::from_str(&schema_json).unwrap();
    assert_eq!(
        parsed.get("primary_key").and_then(Value::as_str),
        Some("id")
    );

    let history_json =
        fs::read_to_string(db_path.join("_metadata").join("schema_history.json")).unwrap();
    let history: Value = serde_json::from_str(&history_json).unwrap();
    assert_eq!(history.as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn test_backup_restore_rolls_back_legacy_upgrade_changes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("legacy-rollback-db");
    let backup_path = temp_dir.path().join("legacy-rollback-backup");
    let restore_path = temp_dir.path().join("legacy-rollback-restored");

    create_legacy_style_database(&db_path).await;

    let legacy_db = DatabaseOps::open(&db_path).await.unwrap();
    legacy_db.backup(&backup_path).await.unwrap();

    let upgraded_db = DatabaseOps::open(&db_path).await.unwrap();
    upgraded_db.set_primary_key("id").unwrap();
    upgraded_db
        .insert(make_batch(&test_schema(), vec![3], vec!["carol"]))
        .await
        .unwrap();
    assert_eq!(count_rows(&upgraded_db).await, 3);

    DatabaseOps::restore(&backup_path, &restore_path)
        .await
        .unwrap();

    let restored_db = DatabaseOps::open(&restore_path).await.unwrap();
    assert_eq!(restored_db.primary_key(), None);
    assert_eq!(count_rows(&restored_db).await, 2);

    let schema_json =
        fs::read_to_string(restore_path.join("_metadata").join("schema.json")).unwrap();
    let parsed: Value = serde_json::from_str(&schema_json).unwrap();
    assert!(parsed.get("primary_key").is_none());
}
