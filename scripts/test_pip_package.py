#!/usr/bin/env python3
"""
Comprehensive integration test for posixlake pip package.

Tests all features using the pip-installed posixlake package (no local path hacks).
Run with: pip install posixlake && python scripts/test_pip_package.py

Features Tested:
1. Database creation (explicit schema, CSV import, Parquet import)
2. Data insertion (JSON, buffered)
3. SQL queries (DataFusion)
4. Time travel
5. Delete operations
6. MERGE (UPSERT) operations
7. Delta Lake operations (OPTIMIZE, VACUUM, Z-ORDER)
8. Monitoring and metrics
9. Authentication and RBAC
10. Backup and restore
11. NFS server (if sudo available)
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

from posixlake import (
    DatabaseOps,
    Field,
    NfsServer,
    PosixLakeError,
    Schema,
    restore,
)


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  ✓ {name}")

    def fail(self, name, error):
        self.failed += 1
        self.errors.append((name, error))
        print(f"  ✗ {name}: {error}")

    def skip(self, name, reason):
        self.skipped += 1
        print(f"  ⊘ {name}: {reason}")

    def summary(self):
        total = self.passed + self.failed + self.skipped
        print(f"\n{'='*70}")
        print(f"  RESULTS: {self.passed}/{total} passed, {self.failed} failed, {self.skipped} skipped")
        print(f"{'='*70}")
        if self.errors:
            print("\nFailures:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        return self.failed == 0


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def test_create_with_schema(temp_dir, results):
    """Test 1: Create database with explicit schema"""
    section("Test 1: Create Database with Schema")

    db_path = os.path.join(temp_dir, "schema_db")
    schema = Schema(fields=[
        Field(name="id", data_type="Int32", nullable=False),
        Field(name="name", data_type="String", nullable=False),
        Field(name="email", data_type="String", nullable=True),
        Field(name="age", data_type="Int32", nullable=True),
        Field(name="salary", data_type="Float64", nullable=True),
    ])

    try:
        db = DatabaseOps.create(db_path, schema)
        results.ok("Create database")

        # Verify schema
        s = db.get_schema()
        assert len(s.fields) == 5, f"Expected 5 fields, got {len(s.fields)}"
        results.ok("Schema verification")

        # Verify Delta Lake structure
        assert os.path.exists(os.path.join(db_path, "_delta_log")), "Missing _delta_log"
        results.ok("Delta Lake structure")

        return db
    except Exception as e:
        results.fail("Create database", str(e))
        return None


def test_create_from_csv(temp_dir, results):
    """Test 2: Create database from CSV with auto schema inference"""
    section("Test 2: Create Database from CSV")

    csv_path = os.path.join(temp_dir, "test_data.csv")
    with open(csv_path, "w") as f:
        f.write("id,name,score,active\n")
        f.write("1,Alice,95.5,true\n")
        f.write("2,Bob,87.3,false\n")
        f.write("3,Charlie,92.1,true\n")

    db_path = os.path.join(temp_dir, "csv_db")

    try:
        db = DatabaseOps.create_from_csv(db_path, csv_path)
        results.ok("Create from CSV")

        # Verify schema inference
        schema = db.get_schema()
        field_types = {f.name: f.data_type for f in schema.fields}

        assert field_types["id"] == "Int64", f"id should be Int64, got {field_types['id']}"
        assert field_types["score"] == "Float64", f"score should be Float64, got {field_types['score']}"
        assert field_types["active"] == "Boolean", f"active should be Boolean, got {field_types['active']}"
        assert field_types["name"] == "String", f"name should be String, got {field_types['name']}"
        results.ok("Schema inference")

        # Verify data
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        rows = json.loads(result)
        assert rows[0]["cnt"] == 3, f"Expected 3 rows, got {rows[0]['cnt']}"
        results.ok("Data verification")

        return db
    except Exception as e:
        results.fail("Create from CSV", str(e))
        return None


def test_create_from_parquet(temp_dir, csv_db, results):
    """Test 3: Create database from Parquet"""
    section("Test 3: Create Database from Parquet")

    if csv_db is None:
        results.skip("Create from Parquet", "CSV database not available")
        return None

    csv_db_path = csv_db.get_base_path()
    parquet_files = [f for f in os.listdir(csv_db_path) if f.endswith(".parquet")]

    if not parquet_files:
        results.skip("Create from Parquet", "No parquet files found")
        return None

    parquet_path = os.path.join(csv_db_path, parquet_files[0])
    db_path = os.path.join(temp_dir, "parquet_db")

    try:
        db = DatabaseOps.create_from_parquet(db_path, parquet_path)
        results.ok("Create from Parquet")

        # Verify data
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        rows = json.loads(result)
        assert rows[0]["cnt"] == 3, f"Expected 3 rows, got {rows[0]['cnt']}"
        results.ok("Data verification")

        return db
    except Exception as e:
        results.fail("Create from Parquet", str(e))
        return None


def test_complex_types(temp_dir, results):
    """Test 4: Complex data types (Decimal, List, Map, Struct)"""
    section("Test 4: Complex Data Types")

    try:
        # Test Decimal128
        db_path = os.path.join(temp_dir, "decimal_db")
        schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="price", data_type="Decimal128(10,2)", nullable=False),
        ])
        db = DatabaseOps.create(db_path, schema)
        s = db.get_schema()
        assert s.fields[1].data_type == "Decimal128(10,2)", f"Expected Decimal128(10,2), got {s.fields[1].data_type}"
        results.ok("Decimal128 type")

        # Test List<Int32>
        db_path = os.path.join(temp_dir, "list_db")
        schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="scores", data_type="List<Int32>", nullable=True),
        ])
        db = DatabaseOps.create(db_path, schema)
        s = db.get_schema()
        assert "List" in s.fields[1].data_type, f"Expected List type, got {s.fields[1].data_type}"
        results.ok("List<Int32> type")

        # Test Struct
        db_path = os.path.join(temp_dir, "struct_db")
        schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="person", data_type="Struct<name:String,age:Int32>", nullable=True),
        ])
        db = DatabaseOps.create(db_path, schema)
        s = db.get_schema()
        assert "Struct" in s.fields[1].data_type, f"Expected Struct type, got {s.fields[1].data_type}"
        results.ok("Struct type")

        # Test Map<String,Int64>
        db_path = os.path.join(temp_dir, "map_db")
        schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="metadata", data_type="Map<String,Int64>", nullable=True),
        ])
        db = DatabaseOps.create(db_path, schema)
        s = db.get_schema()
        assert "Map" in s.fields[1].data_type, f"Expected Map type, got {s.fields[1].data_type}"
        results.ok("Map<String,Int64> type")

    except Exception as e:
        results.fail("Complex types", str(e))


def test_insert_and_query(db, results):
    """Test 5: Insert data and query"""
    section("Test 5: Insert and Query")

    if db is None:
        results.skip("Insert and Query", "Database not available")
        return

    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "salary": 75000.0},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35, "salary": 85000.0},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 28, "salary": 65000.0},
    ]

    try:
        db.insert_json(json.dumps(data))
        results.ok("Insert JSON")

        # Query all
        result = db.query_json("SELECT * FROM data ORDER BY id")
        rows = json.loads(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        results.ok("Query all rows")

        # Query with filter
        result = db.query_json("SELECT name FROM data WHERE age > 30")
        rows = json.loads(result)
        assert len(rows) == 1 and rows[0]["name"] == "Bob"
        results.ok("Query with filter")

        # Aggregation
        result = db.query_json("SELECT AVG(salary) as avg_sal FROM data")
        rows = json.loads(result)
        assert abs(rows[0]["avg_sal"] - 75000.0) < 0.01
        results.ok("Aggregation query")

    except Exception as e:
        results.fail("Insert and Query", str(e))


def test_buffered_insert(db, results):
    """Test 6: Buffered insert"""
    section("Test 6: Buffered Insert")

    if db is None:
        results.skip("Buffered Insert", "Database not available")
        return

    try:
        # Insert multiple small batches
        for i in range(10):
            batch = [{"id": 100 + i, "name": f"User_{i}", "email": f"user{i}@test.com", "age": 25 + i, "salary": 50000.0}]
            db.insert_buffered_json(json.dumps(batch))
        results.ok("Buffered inserts")

        # Flush
        db.flush_write_buffer()
        results.ok("Flush buffer")

        # Verify
        result = db.query_json("SELECT COUNT(*) as cnt FROM data WHERE id >= 100")
        rows = json.loads(result)
        assert rows[0]["cnt"] == 10, f"Expected 10 buffered rows, got {rows[0]['cnt']}"
        results.ok("Verify buffered data")

    except Exception as e:
        results.fail("Buffered Insert", str(e))


def test_time_travel(db, results):
    """Test 7: Time travel"""
    section("Test 7: Time Travel")

    if db is None:
        results.skip("Time Travel", "Database not available")
        return

    try:
        # Get current count
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        current_count = json.loads(result)[0]["cnt"]

        # Insert more data
        db.insert_json('[{"id": 999, "name": "TimeTravel", "email": "tt@test.com", "age": 50, "salary": 100000.0}]')
        results.ok("Insert for time travel")

        # Query current version
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        new_count = json.loads(result)[0]["cnt"]
        assert new_count == current_count + 1
        results.ok("Current version query")

        # Query version 0
        result = db.query_version_json("SELECT COUNT(*) as cnt FROM data", 0)
        v0_count = json.loads(result)[0]["cnt"]
        assert v0_count < new_count, "Version 0 should have fewer rows"
        results.ok("Historical version query")

    except Exception as e:
        results.fail("Time Travel", str(e))


def test_delete(db, results):
    """Test 8: Delete rows"""
    section("Test 8: Delete Rows")

    if db is None:
        results.skip("Delete Rows", "Database not available")
        return

    try:
        # Count before
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        before = json.loads(result)[0]["cnt"]

        # Delete
        db.delete_rows_where("id = 999")
        results.ok("Delete rows")

        # Count after
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        after = json.loads(result)[0]["cnt"]
        assert after == before - 1, f"Expected {before - 1} rows, got {after}"
        results.ok("Verify deletion")

    except Exception as e:
        results.fail("Delete Rows", str(e))


def test_merge(db, results):
    """Test 9: MERGE (UPSERT) operations"""
    section("Test 9: MERGE Operations")

    if db is None:
        results.skip("MERGE", "Database not available")
        return

    try:
        merge_data = [
            {"id": 1, "name": "Alice Updated", "email": "alice.new@test.com", "age": 31, "salary": 80000.0, "_op": "UPDATE"},
            {"id": 200, "name": "NewUser", "email": "new@test.com", "age": 40, "salary": 90000.0, "_op": "INSERT"},
        ]

        result = db.merge_json(json.dumps(merge_data), "id")
        metrics = json.loads(result)
        results.ok("MERGE operation")

        assert metrics["rows_updated"] >= 1, "Should have updated rows"
        assert metrics["rows_inserted"] >= 1, "Should have inserted rows"
        results.ok("MERGE metrics")

    except Exception as e:
        results.fail("MERGE", str(e))


def test_delta_operations(db, results):
    """Test 10: Delta Lake operations"""
    section("Test 10: Delta Lake Operations")

    if db is None:
        results.skip("Delta Operations", "Database not available")
        return

    try:
        db.optimize()
        results.ok("OPTIMIZE")

        files = db.vacuum_dry_run(0)
        results.ok(f"VACUUM dry run ({len(files)} files)")

        db.zorder(["age"])
        results.ok("Z-ORDER")

    except Exception as e:
        results.fail("Delta Operations", str(e))


def test_monitoring(db, results):
    """Test 10: Monitoring and metrics"""
    section("Test 10: Monitoring")

    if db is None:
        results.skip("Monitoring", "Database not available")
        return

    try:
        metrics = db.get_metrics()
        assert metrics.total_queries > 0, "Should have queries"
        results.ok("Get metrics")

        health = db.health_check()
        assert health.status == "healthy", f"Expected healthy, got {health.status}"
        results.ok("Health check")

        stats = db.get_data_skipping_stats()
        results.ok("Data skipping stats")

    except Exception as e:
        results.fail("Monitoring", str(e))


def test_auth_rbac(temp_dir, results):
    """Test 11: Authentication and RBAC"""
    section("Test 11: Authentication & RBAC")

    db_path = os.path.join(temp_dir, "auth_db")
    schema = Schema(fields=[
        Field(name="id", data_type="Int32", nullable=False),
        Field(name="name", data_type="String", nullable=False),
    ])

    try:
        # Create with auth
        auth_db = DatabaseOps.create_with_auth(db_path, schema, True)
        results.ok("Create with auth")

        # Create users
        auth_db.create_user("admin", "admin123", ["admin"])
        auth_db.create_user("reader", "reader123", ["read"])
        results.ok("Create users")

        # Insert as admin
        auth_db.insert_json('[{"id": 1, "name": "Test"}]')
        results.ok("Admin insert")

        auth_db = None  # Close

        # Open as reader
        reader_db = DatabaseOps.open_with_credentials(db_path, "reader", "reader123")
        results.ok("Reader authentication")

        # Reader can read
        result = reader_db.query_json("SELECT * FROM data")
        results.ok("Reader query")

        # Reader cannot write
        try:
            reader_db.insert_json('[{"id": 2, "name": "Unauthorized"}]')
            results.fail("Permission enforcement", "Reader should not be able to insert")
        except PosixLakeError:
            results.ok("Permission enforcement")

    except Exception as e:
        results.fail("Auth/RBAC", str(e))


def test_backup_restore(temp_dir, db, results):
    """Test 12: Backup and restore"""
    section("Test 12: Backup & Restore")

    if db is None:
        results.skip("Backup/Restore", "Database not available")
        return

    backup_path = os.path.join(temp_dir, "backup")
    restore_path = os.path.join(temp_dir, "restored")

    try:
        db.backup(backup_path)
        results.ok("Create backup")

        restore(backup_path, restore_path)
        results.ok("Restore backup")

        restored_db = DatabaseOps.open(restore_path)
        result = restored_db.query_json("SELECT COUNT(*) as cnt FROM data")
        rows = json.loads(result)
        assert rows[0]["cnt"] > 0, "Restored DB should have data"
        results.ok("Verify restored data")

    except Exception as e:
        results.fail("Backup/Restore", str(e))


def test_nfs_server(db, results):
    """Test 13: NFS server (requires sudo)"""
    section("Test 13: NFS Server")

    if db is None:
        results.skip("NFS Server", "Database not available")
        return

    try:
        nfs = NfsServer(db, 12199)
        results.ok("Start NFS server")

        time.sleep(0.5)
        if nfs.is_ready():
            results.ok("NFS server ready")
        else:
            results.fail("NFS server ready", "Server not ready")

        nfs.shutdown()
        results.ok("Shutdown NFS server")

    except Exception as e:
        results.fail("NFS Server", str(e))


def main():
    print("=" * 70)
    print("  posixlake pip package integration test")
    print("=" * 70)

    # Check package version
    try:
        import posixlake
        print(f"\nTesting posixlake package")
    except ImportError as e:
        print(f"\n✗ posixlake not installed: {e}")
        print("Install with: pip install posixlake")
        sys.exit(1)

    results = TestResult()
    temp_dir = tempfile.mkdtemp(prefix="posixlake_test_")

    try:
        # Run all tests
        db = test_create_with_schema(temp_dir, results)
        csv_db = test_create_from_csv(temp_dir, results)
        test_create_from_parquet(temp_dir, csv_db, results)
        test_complex_types(temp_dir, results)
        test_insert_and_query(db, results)
        test_buffered_insert(db, results)
        test_time_travel(db, results)
        test_delete(db, results)
        test_merge(db, results)
        test_delta_operations(db, results)
        test_monitoring(db, results)
        test_auth_rbac(temp_dir, results)
        test_backup_restore(temp_dir, db, results)
        test_nfs_server(db, results)

    finally:
        # Cleanup
        print("\nCleaning up...")
        try:
            shutil.rmtree(temp_dir)
            print(f"✓ Removed {temp_dir}")
        except Exception as e:
            print(f"✗ Cleanup failed: {e}")

    # Summary
    success = results.summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
