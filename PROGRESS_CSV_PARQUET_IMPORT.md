# Project Plan: CSV & Parquet Import Support

## Current State
- **CLI**: Has `mount`, `unmount`, `status`, `s3test` commands — **no `create` command**
- **Core**: `DatabaseOps::create()` requires explicit schema, no import from files
- **Existing code**:
  - `ParquetReader` in `storage/parquet.rs` — can read raw Parquet files
  - `arrow::csv::ReaderBuilder` already used in `nfs/file_views.rs` and `nfs/write_buffer.rs`
  - `deltalake` crate available with full Delta Lake support

---

## Implementation Tree

### 1. Rust Core (`database_ops.rs`)
- [x] **1.1** Add `create_from_csv(path, csv_file)` ✅ DONE
  - **Auto schema inference** from first 10 data rows (excluding header):
    - Try parse as `Int64` → if all 10 succeed, use `Int64`
    - Try parse as `Float64` → if all 10 succeed, use `Float64`
    - Try parse as `Boolean` (true/false/1/0) → if all 10 succeed, use `Boolean`
    - Otherwise → default to `String`
  - Column names from CSV header row
  - All columns nullable by default
  - Create Delta Lake table with inferred schema
  - Insert all CSV rows
- [x] **1.2** Add `create_from_parquet(path, parquet_files)` ✅ DONE
  - Read schema directly from Parquet file metadata (no inference needed)
  - Convert to Delta Lake (add `_delta_log/`)
  - Support single file or glob pattern

### 2. CLI (`bin/posixlake.rs`)
- [ ] **2.1** Add `Create` subcommand
  ```bash
  posixlake create <DB_PATH> --schema "id:Int32,name:String"
  ```
- [ ] **2.2** Add `--from-csv` option
  ```bash
  posixlake create <DB_PATH> --from-csv data.csv
  ```
- [ ] **2.3** Add `--from-parquet` option
  ```bash
  posixlake create <DB_PATH> --from-parquet "*.parquet"
  ```

### 3. Python Bindings (`python.rs` + UniFFI)
- [ ] **3.1** Expose `DatabaseOps.create_from_csv()`
- [ ] **3.2** Expose `DatabaseOps.create_from_parquet()`

### 4. Tests
- [x] **4.1** Unit tests for CSV import (schema inference, data integrity) ✅ DONE
- [x] **4.2** Unit tests for Parquet import (single file, glob patterns) ✅ DONE
- [ ] **4.3** Integration tests (CLI end-to-end)

### 5. Documentation
- [ ] **5.1** Update CLI `--help`
- [ ] **5.2** Update README with examples

---

## Next Steps
1. ~~Start with **1.1** and **1.2** (Rust core functions)~~ ✅ DONE
2. Then **2.1-2.3** (CLI integration) ← **NEXT**
3. Then **3.1-3.2** (Python bindings)
4. ~~Finally **4.x** and **5.x** (tests and docs)~~ Tests done, docs pending
