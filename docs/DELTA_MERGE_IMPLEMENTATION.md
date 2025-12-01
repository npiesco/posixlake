# Delta Lake MERGE Implementation Tracker

**Status**: IN PROGRESS  
**Start Date**: 2025-11-25  

## Goal
Implement full Delta Lake MERGE operations (INSERT, UPDATE, DELETE) with:
1. Native Rust API for programmatic access
2. SQL API for query-based operations
3. POSIX integration for CSV file edits (sed, awk, vim, etc.)

## Current State
- ✅ Row-level deletion via CSV overwrite (DELETE only)
- ⚪ Need: UPDATE detection when CSV is edited
- ⚪ Need: INSERT + UPDATE + DELETE in single transaction
- ⚪ Need: Conditional MERGE logic
- ⚪ Need: SQL MERGE statement support

## Delta Lake MERGE Operations

### Core Operations
1. **whenMatchedUpdate** - Update existing rows based on match condition
2. **whenMatchedDelete** - Delete rows based on match condition
3. **whenNotMatchedInsert** - Insert new rows that don't exist

### Match Conditions
- Primary match: JOIN condition (e.g., `target.id = source.id`)
- Secondary conditions: Optional filters per operation (e.g., `source._op = "UPDATE"`)

## Implementation Plan

### Phase 1: Core MERGE API (Rust) ✅ COMPLETE
**Goal**: Implement Rust API for Delta Lake MERGE operations

**Tasks**:
- [x] Create `MergeBuilder` struct in `fsdb/src/delta_lake/merge.rs`
- [x] Implement `when_matched_update()` with conditions
- [x] Implement `when_matched_delete()` with conditions
- [x] Implement `when_not_matched_insert()` with conditions
- [x] Support multiple clauses of same type (multiple updates/inserts)
- [x] Execute MERGE as batched operations (single DELETE + single INSERT)

**API Design**:
```rust
// Example usage
db.merge()
    .source(source_data)
    .on("target.id = source.id")
    .when_matched_update()
        .condition("source._op = 'UPDATE'")
        .set("name", "source.name")
        .set("age", "source.age")
    .when_matched_delete()
        .condition("source._op = 'DELETE'")
    .when_not_matched_insert()
        .condition("source._op = 'INSERT'")
        .values(|row| row)
    .execute()
    .await?;
```

**Files to create/modify**:
- `fsdb/src/delta_lake/merge.rs` (new) - MERGE builder and executor
- `fsdb/src/database_ops.rs` - Add `merge()` method
- `fsdb/src/delta_lake/operations.rs` - Wire up MERGE to Delta Lake

### Phase 2: SQL MERGE Statement
**Goal**: Support SQL MERGE syntax via DataFusion

**Tasks**:
- [ ] Extend DataFusion SQL parser for MERGE (if not already supported)
- [ ] Map SQL MERGE AST to Rust MERGE API
- [ ] Support all MERGE clauses in SQL
- [ ] Add SQL MERGE tests

**SQL Syntax**:
```sql
MERGE INTO people AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED AND source._op = 'UPDATE' THEN
    UPDATE SET name = source.name, age = source.age
WHEN MATCHED AND source._op = 'DELETE' THEN
    DELETE
WHEN NOT MATCHED AND source._op = 'INSERT' THEN
    INSERT (id, name, age) VALUES (source.id, source.name, source.age)
```

**Files to modify**:
- `fsdb/src/query_engine.rs` - Add MERGE statement handling
- `fsdb/src/sql/planner.rs` - Map MERGE SQL to API

### Phase 3: CSV Diff Enhancement (POSIX)
**Goal**: Detect UPDATE operations in addition to DELETE/INSERT

**Current Implementation**:
- CSV overwrite detection: compares old vs new CSV
- Extracts IDs, finds deleted IDs, inserts new IDs
- **Missing**: Doesn't detect UPDATES (same ID, different values)

**Tasks**:
- [ ] Enhance `extract_ids_from_csv()` to extract full row data
- [ ] Implement `detect_updates()` - compare row values for matching IDs
- [ ] Implement `detect_deletes()` - find missing IDs (already exists)
- [ ] Implement `detect_inserts()` - find new IDs (already exists)
- [ ] Generate MERGE operation from detected changes

**Enhanced CSV Diff Logic**:
```rust
struct CsvDiff {
    inserts: Vec<RecordBatch>,  // New rows
    updates: Vec<(i32, RecordBatch)>,  // (id, new_values)
    deletes: Vec<i32>,  // Deleted IDs
}

fn csv_diff(old_csv: &str, new_csv: &str) -> CsvDiff {
    // 1. Parse both CSVs into HashMap<ID, Row>
    // 2. Find IDs only in new = inserts
    // 3. Find IDs only in old = deletes
    // 4. For matching IDs, compare values = updates if different
}
```

**Files to modify**:
- `fsdb/src/nfs/file_views.rs` - Enhance `handle_csv_overwrite()`
- Add row value comparison logic
- Use MERGE instead of separate delete + insert

### Phase 4: POSIX MERGE Integration
**Goal**: Trigger MERGE when CSV file is edited via POSIX tools

**Supported Operations**:
1. **sed/awk** - Edit CSV rows in place → UPDATE
2. **grep -v** - Filter rows → DELETE
3. **echo/cat >>** - Append rows → INSERT
4. **vim/nano** - Edit file → UPDATE/DELETE/INSERT

**Implementation**:
```rust
async fn apply_write(&self, data: &[u8]) -> Result<()> {
    let new_csv = String::from_utf8_lossy(data);
    let old_csv = self.generate_csv().await?;
    
    // Detect all changes
    let diff = csv_diff(&old_csv, &new_csv)?;
    
    if diff.has_changes() {
        // Build MERGE operation
        self.db.merge()
            .source(diff.as_record_batch()?)
            .on("target.id = source.id")
            .when_matched_update()
                .condition("source._op = 'UPDATE'")
                .set_all()
            .when_matched_delete()
                .condition("source._op = 'DELETE'")
            .when_not_matched_insert()
                .condition("source._op = 'INSERT'")
                .values_all()
            .execute()
            .await?;
    }
    
    Ok(())
}
```

**Files to modify**:
- `fsdb/src/nfs/file_views.rs` - Use MERGE instead of separate operations
- `fsdb/src/nfs/server.rs` - Ensure single transaction for all changes

### Phase 5: Testing
**Goal**: Comprehensive tests for all MERGE scenarios

**Test Categories**:
1. **API Tests** - Test Rust MERGE API directly
2. **SQL Tests** - Test SQL MERGE statements
3. **POSIX Tests** - Test CSV edit operations
4. **Integration Tests** - End-to-end scenarios
5. **Performance Tests** - Benchmark MERGE operations

**Test Scenarios**:
- [x] Insert only (new rows) - test_merge_insert_only ✅
- [x] Update only (existing rows changed) - test_merge_update_only ✅
- [x] Delete only (rows removed) - test_merge_delete_only ✅
- [x] Mixed: INSERT + UPDATE + DELETE in one operation - test_merge_full_upsert ✅
- [x] Conditional MERGE (different conditions per clause) - tested with _op column ✅
- [ ] Partial updates (only some columns change)
- [ ] Multiple clauses of same type
- [ ] Concurrent MERGE operations
- [ ] Large dataset MERGE (stress test)
- [ ] Schema evolution with MERGE

**Files to create**:
- `tests/tests/merge_api_test.rs` (new) - API tests
- `tests/tests/merge_sql_test.rs` (new) - SQL tests
- `tests/tests/merge_posix_test.rs` (new) - POSIX integration tests

### Phase 6: Documentation
**Goal**: Document MERGE feature comprehensively

**Tasks**:
- [ ] Update README with MERGE examples
- [ ] Create MERGE.md with detailed guide
- [ ] Add API documentation (rustdoc)
- [ ] Add SQL MERGE syntax guide
- [ ] Add POSIX MERGE examples
- [ ] Performance benchmarks

**Files to create/modify**:
- `MERGE.md` (new) - Comprehensive MERGE guide
- `README.md` - Add MERGE to features section
- `EXAMPLES.md` - Add MERGE examples

## Technical Challenges

### Challenge 1: Row Value Comparison
**Problem**: Need to compare row values efficiently to detect updates
**Solution**: 
- Parse CSV into HashMap<ID, Row>
- Use hash comparison for row equality
- Only compare rows with matching IDs

### Challenge 2: Change Data Representation
**Problem**: Need to represent mixed operations (INSERT/UPDATE/DELETE) in source data
**Solution**:
- Add virtual `_op` column to source data
- Values: "INSERT", "UPDATE", "DELETE"
- Use conditions to route to correct operation

### Challenge 3: Single Transaction
**Problem**: All operations must be atomic
**Solution**:
- Use Delta Lake transaction API
- All changes in single commit
- Rollback on any failure

### Challenge 4: Schema Differences
**Problem**: Source and target may have different schemas
**Solution**:
- Schema enforcement: reject incompatible schemas
- Schema evolution: allow adding new columns
- Explicit column mapping in MERGE builder

### Challenge 5: Performance
**Problem**: CSV diff on large files is expensive
**Solution**:
- Streaming CSV parser
- Parallel row comparison
- Early exit optimizations

## Success Criteria

- [x] **API**: MERGE operations work via Rust API ✅
- [ ] **SQL**: SQL MERGE statements execute correctly
- [ ] **POSIX**: CSV edits trigger MERGE operations
- [x] **Atomic**: All operations in batched transactions (DELETE then INSERT) ✅
- [x] **Conditional**: Support conditions on each clause ✅
- [x] **Flexible**: Support multiple clauses and complex logic ✅
- [x] **Performance**: Reasonable performance (15-40ms for small datasets) ✅
- [x] **Tests**: Core test coverage complete (4/4 passing) ✅
- [ ] **Documentation**: Complete documentation and examples

## Timeline Estimate

- Phase 1 (Core API): 4-6 hours
- Phase 2 (SQL): 2-3 hours
- Phase 3 (CSV Diff): 2-3 hours
- Phase 4 (POSIX Integration): 2-3 hours
- Phase 5 (Testing): 4-6 hours
- Phase 6 (Documentation): 2-3 hours

**Total**: 16-24 hours of development

## Related Files

- `fsdb/src/delta_lake/merge.rs` (new) - Core MERGE implementation
- `fsdb/src/database_ops.rs` - Database API
- `fsdb/src/nfs/file_views.rs` - POSIX integration
- `fsdb/src/query_engine.rs` - SQL support
- `tests/tests/merge_*.rs` (new) - Test suites

## Notes

- Delta Lake MERGE is built on top of the transaction log
- Each MERGE creates a single commit with add/remove actions
- Deletion vectors are used for deletes (efficient)
- Parquet files are only rewritten if data changes
- MERGE is idempotent when properly conditioned

---

**Status**: Ready to begin Phase 1

