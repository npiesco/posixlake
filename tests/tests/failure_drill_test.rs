//! Failure drill tests — validate crash recovery and backup/restore under adversarial conditions

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use posixlake::DatabaseOps;
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

/// Drill: Corrupt delta log mid-write, then recover via health check
#[tokio::test]
async fn test_recovery_after_partial_delta_log_corruption() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("drill_corrupt");
    let schema = test_schema();

    let db = DatabaseOps::create(&db_path, schema.clone())
        .await
        .expect("create should succeed");

    // Insert baseline data
    db.insert(make_batch(&schema, vec![1, 2, 3], vec!["a", "b", "c"]))
        .await
        .expect("baseline insert should succeed");

    // Verify data is queryable before corruption
    let pre = db.query("SELECT COUNT(*) as cnt FROM data").await.unwrap();
    assert!(!pre.is_empty(), "should have results before corruption");

    // Simulate partial corruption: append garbage to the latest commit JSON
    let delta_log = db_path.join("_delta_log");
    let mut entries: Vec<_> = std::fs::read_dir(&delta_log)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    if let Some(latest) = entries.last() {
        // Append garbage — the JSON is no longer valid
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(latest.path())
            .unwrap();
        writeln!(f, "{{CORRUPTED GARBAGE}}").unwrap();
    }

    // Reopen — Delta Lake should still be able to read the table
    // because each line in the commit log is parsed independently
    let db2 = DatabaseOps::open(&db_path).await;
    // The open may succeed or fail depending on how delta-rs handles trailing garbage.
    // Either outcome is acceptable for this drill — we just verify no panic/hang.
    match db2 {
        Ok(db2) => {
            let health = db2.health_check().await;
            // Should report some status (healthy or degraded), not crash
            assert!(
                !health.status.is_empty(),
                "health status should be non-empty after corruption recovery"
            );
        }
        Err(e) => {
            // Graceful error is acceptable — no panic
            eprintln!("Open after corruption returned error (acceptable): {}", e);
        }
    }
}

/// Drill: Backup, destroy original, restore, verify data
#[tokio::test]
async fn test_backup_restore_drill() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("drill_backup");
    let backup_path = tmp.path().join("drill_backup_archive");
    let restore_path = tmp.path().join("drill_restored");
    let schema = test_schema();

    // Create and populate
    let db = DatabaseOps::create(&db_path, schema.clone())
        .await
        .expect("create should succeed");
    db.insert(make_batch(
        &schema,
        vec![1, 2, 3],
        vec!["alice", "bob", "charlie"],
    ))
    .await
    .expect("insert should succeed");
    db.insert(make_batch(&schema, vec![4, 5], vec!["dave", "eve"]))
        .await
        .expect("second insert should succeed");

    // Take backup
    db.backup(&backup_path)
        .await
        .expect("backup should succeed");
    assert!(backup_path.exists(), "backup directory should exist");

    // Destroy original
    drop(db);
    std::fs::remove_dir_all(&db_path).expect("should be able to remove original");
    assert!(!db_path.exists(), "original should be gone");

    // Restore from backup
    DatabaseOps::restore(&backup_path, &restore_path)
        .await
        .expect("restore should succeed");

    // Open restored and verify data
    let restored = DatabaseOps::open(&restore_path)
        .await
        .expect("open restored should succeed");

    let results = restored
        .query("SELECT id, name FROM data ORDER BY id")
        .await
        .expect("query restored should succeed");
    assert!(!results.is_empty(), "restored DB should have data");

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "restored DB should have all 5 rows");

    let health = restored.health_check().await;
    assert_eq!(health.status, "healthy", "restored DB should be healthy");
}

/// Drill: Concurrent writes don't corrupt data
#[tokio::test]
async fn test_concurrent_write_safety() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("drill_concurrent");
    let schema = test_schema();

    let db = Arc::new(
        DatabaseOps::create(&db_path, schema.clone())
            .await
            .expect("create should succeed"),
    );

    // Spawn multiple concurrent inserts
    let mut handles = Vec::new();
    for i in 0..5 {
        let db = db.clone();
        let schema = schema.clone();
        handles.push(tokio::spawn(async move {
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![i * 10 + 1, i * 10 + 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        format!("name_{}a", i),
                        format!("name_{}b", i),
                    ])) as ArrayRef,
                ],
            )
            .unwrap();
            db.insert(batch).await
        }));
    }

    // Collect results — some may fail due to concurrency conflicts, that's OK
    let mut successes = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            successes += 1;
        }
    }

    // At least some inserts should succeed
    assert!(
        successes > 0,
        "at least one concurrent insert should succeed"
    );

    // DB should still be healthy and queryable
    let health = db.health_check().await;
    assert_eq!(
        health.status, "healthy",
        "DB should be healthy after concurrent writes"
    );

    let results = db.query("SELECT COUNT(*) as cnt FROM data").await;
    assert!(
        results.is_ok(),
        "query should succeed after concurrent writes"
    );
}
