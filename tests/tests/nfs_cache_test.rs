//! NFS Cache Integration Tests
//!
//! Tests for incremental cache invalidation and per-partition caching.
//! Track 3: Cache should avoid full regeneration after partial writes.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use posixlake::database_ops::DatabaseOps;
use posixlake::nfs::cache::NfsCache;
use posixlake::nfs::file_views::CsvFileView;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("posixlake=info")
        .try_init();
}

/// Helper: create a database with N rows across multiple inserts (creating multiple Parquet files)
async fn create_db_with_multiple_parquet_files(
    db_path: &std::path::Path,
    num_inserts: usize,
    rows_per_insert: usize,
) -> Arc<DatabaseOps> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();
    let mut next_id = 1i32;

    for _ in 0..num_inserts {
        let ids: Vec<i32> = (next_id..next_id + rows_per_insert as i32).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("user_{}", id)).collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.clone())),
                Arc::new(StringArray::from(name_refs)),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
        next_id += rows_per_insert as i32;
    }

    Arc::new(db)
}

/// RED TEST 3a: After an append, cache should be partially reused (not fully regenerated)
///
/// Current behavior: cache is fully invalidated on any write, next read regenerates everything.
/// Desired behavior: append should extend cached CSV without regenerating old data.
#[tokio::test]
async fn test_cache_partial_invalidation_after_append() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_cache_append");
    let cache_path = temp_dir.path().join("cache_db");

    // Create DB with many rows across multiple Parquet files
    let db = create_db_with_multiple_parquet_files(&db_path, 5, 200).await;
    let cache = NfsCache::new(&cache_path).await.unwrap();

    // First read: generate and cache full CSV
    let csv_view = CsvFileView::new(db.clone());
    let full_csv = csv_view.generate_csv().await.unwrap();
    cache
        .insert("csv:data".to_string(), full_csv.clone())
        .await
        .unwrap();

    let cached = cache.get("csv:data").await.unwrap().unwrap();
    assert_eq!(
        cached.len(),
        full_csv.len(),
        "Cache should contain full CSV"
    );

    // Append 10 new rows
    let schema = db.schema();
    let append_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,
            ])),
            Arc::new(StringArray::from(vec![
                "new_1", "new_2", "new_3", "new_4", "new_5", "new_6", "new_7", "new_8", "new_9",
                "new_10",
            ])),
        ],
    )
    .unwrap();
    db.insert(append_batch).await.unwrap();

    // The cache should support incremental update:
    // Generate CSV only for the NEW parquet file and append to cached content
    let new_csv = csv_view.generate_csv_incremental(&cache).await.unwrap();
    cache
        .insert("csv:data".to_string(), new_csv.clone())
        .await
        .unwrap();

    // New CSV should be longer than old (has more rows)
    assert!(
        new_csv.len() > full_csv.len(),
        "Updated CSV ({} bytes) should be larger than original ({} bytes)",
        new_csv.len(),
        full_csv.len()
    );

    // Verify new rows are present
    let csv_str = String::from_utf8(new_csv).unwrap();
    assert!(csv_str.contains("new_10"), "New rows should be in the CSV");
    assert!(
        csv_str.contains("user_1"),
        "Old rows should still be present"
    );
}

/// RED TEST 3c: Cache should track per-partition chunks and only regenerate changed ones
///
/// Current behavior: single monolithic cache entry for all data.
/// Desired behavior: per-Parquet-file chunks, selective regeneration.
#[tokio::test]
async fn test_cache_regenerates_only_changed_partitions() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_cache_partitions");
    let cache_path = temp_dir.path().join("cache_db_partitions");

    // Create DB with 5 Parquet files
    let db = create_db_with_multiple_parquet_files(&db_path, 5, 100).await;
    let cache = NfsCache::new(&cache_path).await.unwrap();
    let csv_view = CsvFileView::new(db.clone());

    // Populate per-partition cache
    let chunk_keys = csv_view.populate_chunk_cache(&cache).await.unwrap();
    assert_eq!(
        chunk_keys.len(),
        5,
        "Should have 5 cached chunks (one per Parquet file)"
    );

    // Verify each chunk is cached
    for key in &chunk_keys {
        let cached = cache.get(key).await.unwrap();
        assert!(cached.is_some(), "Chunk {} should be cached", key);
    }

    // Insert new data (creates a 6th Parquet file)
    let schema = db.schema();
    let new_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![501, 502])),
            Arc::new(StringArray::from(vec!["extra_1", "extra_2"])),
        ],
    )
    .unwrap();
    db.insert(new_batch).await.unwrap();

    // Refresh cache incrementally — should only generate the new chunk
    let updated_keys = csv_view.refresh_chunk_cache(&cache).await.unwrap();

    // Only 1 new chunk should have been generated
    assert_eq!(
        updated_keys.len(),
        1,
        "Only the new Parquet file's chunk should be regenerated, got {}",
        updated_keys.len()
    );

    // Total chunks should now be 6
    let all_keys = csv_view.list_chunk_keys(&cache).await.unwrap();
    assert_eq!(all_keys.len(), 6, "Should have 6 total chunks");
}

// =============================================================================
// Track 4: Smarter write-path diffing
// =============================================================================

/// RED TEST 4a: Append should NOT generate full CSV for comparison
///
/// Current: apply_write always generates/fetches full CSV even for appends.
/// Desired: append_only writes skip CSV generation entirely.
#[tokio::test]
async fn test_append_skips_full_csv_diff() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_append_skip");

    // Create DB with many rows
    let db = create_db_with_multiple_parquet_files(&db_path, 3, 500).await;
    let csv_view = CsvFileView::new(db.clone());

    // Append raw CSV data (no header = append mode)
    let append_data = b"1501,appended_user\n1502,appended_user_2\n";

    // This should work WITHOUT needing cached CSV and without generating full CSV
    // The offset-aware write should detect pure append and skip diff
    csv_view.apply_write_append_only(append_data).await.unwrap();

    // Verify the new rows were inserted
    let results = db
        .query("SELECT COUNT(*) as count FROM data WHERE id >= 1501")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Should have appended 2 rows");
}

/// RED TEST 4c: Overwrite diff should narrow to changed region
///
/// Current: full CSV-to-CSV diff on overwrite.
/// Desired: byte-range comparison to find changed rows, then diff only those.
#[tokio::test]
async fn test_overwrite_diffs_only_changed_byte_ranges() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_narrow_diff");

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = Arc::new(DatabaseOps::create(&db_path, schema.clone()).await.unwrap());
    let initial = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
        ],
    )
    .unwrap();
    db.insert(initial).await.unwrap();

    let csv_view = CsvFileView::new(db.clone());

    // Generate the "old" CSV (what's cached)
    let old_csv = csv_view.generate_csv().await.unwrap();
    let old_csv_str = String::from_utf8(old_csv.clone()).unwrap();

    // Create "new" CSV with one row changed (Bob -> BOB_UPDATED)
    let new_csv_str = old_csv_str.replace("Bob", "BOB_UPDATED");

    // Detect which rows changed using byte-range diff
    let changed = CsvFileView::diff_csv_bytes(old_csv_str.as_bytes(), new_csv_str.as_bytes());
    assert!(changed.is_some(), "Should detect changed region");

    let (changed_old_rows, changed_new_rows) = changed.unwrap();
    // Should have identified just the one changed row, not all rows
    assert_eq!(
        changed_new_rows.len(),
        1,
        "Should detect exactly 1 changed row, got {}",
        changed_new_rows.len()
    );
    assert!(
        changed_new_rows[0].contains("BOB_UPDATED"),
        "Changed row should contain the update"
    );
    assert_eq!(
        changed_old_rows.len(),
        1,
        "Should detect exactly 1 old row that changed"
    );
}

// =============================================================================
// Track 5: Streaming/chunked CSV reads
// =============================================================================

/// RED TEST 5a: Reading first N bytes should not generate CSV for the entire table
///
/// Current: CsvFileView::read() calls generate_csv() which does SELECT * FROM data.
/// Desired: chunk-based generation that only reads Parquet files needed for the range.
#[tokio::test]
async fn test_read_first_n_bytes_without_full_generation() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_chunk_read");
    let cache_path = temp_dir.path().join("cache_chunk_read");

    // Create DB with 5 Parquet files (100 rows each)
    let db = create_db_with_multiple_parquet_files(&db_path, 5, 100).await;
    let cache = NfsCache::new(&cache_path).await.unwrap();
    let csv_view = CsvFileView::new(db.clone());

    // Read first 512 bytes via chunk-aware read
    let first_bytes = csv_view.read_chunked(0, 512, &cache).await.unwrap();

    // Should get CSV header + some rows
    let csv_str = String::from_utf8(first_bytes.clone()).unwrap();
    assert!(
        csv_str.starts_with("id,name"),
        "Should start with CSV header"
    );
    assert!(csv_str.contains("user_"), "Should contain data rows");
    assert!(first_bytes.len() <= 512, "Should not exceed requested size");

    // Only the first Parquet file chunk should be cached (not all 5)
    let cached_chunks = csv_view.list_chunk_keys(&cache).await.unwrap();
    assert!(
        cached_chunks.len() < 5,
        "Should NOT cache all 5 chunks for a small read, got {}",
        cached_chunks.len()
    );
}

/// RED TEST 5c: Head command (read offset=0, small size) should only query first Parquet file
#[tokio::test]
async fn test_head_command_reads_only_first_chunk() {
    setup_logging();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_head_chunk");
    let cache_path = temp_dir.path().join("cache_head_chunk");

    // Create DB with 5 Parquet files
    let db = create_db_with_multiple_parquet_files(&db_path, 5, 100).await;
    let cache = NfsCache::new(&cache_path).await.unwrap();
    let csv_view = CsvFileView::new(db.clone());

    // Simulate `head -1` by reading a small amount
    let head_bytes = csv_view.read_chunked(0, 256, &cache).await.unwrap();

    let csv_str = String::from_utf8(head_bytes).unwrap();
    assert!(csv_str.starts_with("id,name"), "Should have CSV header");

    // At most 1-2 chunks should be cached (first file + maybe partial second)
    let cached = csv_view.list_chunk_keys(&cache).await.unwrap();
    assert!(
        cached.len() <= 2,
        "Head command should cache at most 2 chunks, got {}",
        cached.len()
    );
}
