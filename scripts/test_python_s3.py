#!/usr/bin/env python3
"""
Integration tests for posixlake Python S3 bindings via UniFFI.

Runs against a local MinIO instance on localhost:9000 and exercises the Python
binding surface from the repository's uv environment.
"""

import json
import socket
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BINDINGS_DIR = REPO_ROOT / "bindings" / "python"

if str(PYTHON_BINDINGS_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_BINDINGS_DIR))

from posixlake import DatabaseOps, Field, HealthStatus, PosixLakeError, S3Config, Schema

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "posixlake-test"
S3_REGION = "us-east-1"


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
        print(f"\n{'=' * 70}")
        print(f"  RESULTS: {self.passed}/{total} passed, {self.failed} failed, {self.skipped} skipped")
        print(f"{'=' * 70}")
        if self.errors:
            print("\nFailures:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        return self.failed == 0


def section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def minio_reachable():
    try:
        sock = socket.create_connection(("127.0.0.1", 9000), timeout=2)
        sock.close()
        return True
    except OSError:
        return False


def s3_config():
    return S3Config(
        endpoint=S3_ENDPOINT,
        access_key_id=S3_ACCESS_KEY,
        secret_access_key=S3_SECRET_KEY,
        region=S3_REGION,
    )


def unique_s3_path():
    return f"s3://{S3_BUCKET}/python_s3_{int(time.time() * 1000)}"


def iot_schema():
    return Schema(
        fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="sensor", data_type="String", nullable=False),
            Field(name="reading", data_type="Float64", nullable=False),
            Field(name="location", data_type="String", nullable=True),
        ],
        primary_key="id",
    )


def test_create_with_s3(results):
    section("Test 1: Create S3 Database")
    try:
        db = DatabaseOps.create_with_s3(unique_s3_path(), iot_schema(), s3_config())
        if db is None:
            raise RuntimeError("DatabaseOps.create_with_s3 returned None")
        schema = db.get_schema()
        field_names = [field.name for field in schema.fields]
        assert field_names == ["id", "sensor", "reading", "location"]
        results.ok("create_with_s3")
        results.ok("schema round-trip")
    except Exception as e:
        results.fail("create_with_s3", str(e))


def test_create_with_bad_endpoint(results):
    section("Test 2: Bad S3 Endpoint")
    try:
        bad_config = S3Config(
            endpoint="http://localhost:19999",
            access_key_id=S3_ACCESS_KEY,
            secret_access_key=S3_SECRET_KEY,
            region=S3_REGION,
        )
        try:
            DatabaseOps.create_with_s3(unique_s3_path(), iot_schema(), bad_config)
            results.fail("bad endpoint error", "expected PosixLakeError")
        except PosixLakeError:
            results.ok("bad endpoint error")
    except Exception as e:
        results.fail("bad endpoint error", str(e))


def test_open_with_s3(results):
    section("Test 3: Open S3 Database")
    try:
        path = unique_s3_path()
        db = DatabaseOps.create_with_s3(path, iot_schema(), s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"}
        ]))
        reopened = DatabaseOps.open_with_s3(path, s3_config())
        rows = json.loads(reopened.query_json("SELECT * FROM data"))
        assert len(rows) == 1
        assert rows[0]["sensor"] == "temp_01"
        results.ok("open_with_s3")
    except Exception as e:
        results.fail("open_with_s3", str(e))


def test_health_check(results):
    section("Test 4: S3 Health Check")
    try:
        db = DatabaseOps.create_with_s3(unique_s3_path(), iot_schema(), s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"}
        ]))
        health = db.health_check()
        assert isinstance(health, HealthStatus)
        assert health.status in ("healthy", "not_initialized")
        assert health.uptime_seconds >= 0
        results.ok("health_check type")
        results.ok("health_check values")
    except Exception as e:
        results.fail("health_check", str(e))


def test_insert_query_reopen(results):
    section("Test 5: Insert, Query, Reopen")
    try:
        path = unique_s3_path()
        db = DatabaseOps.create_with_s3(path, iot_schema(), s3_config())
        inserted = db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"},
            {"id": 2, "sensor": "humidity_02", "reading": 55.0, "location": "rack_B"},
            {"id": 3, "sensor": "pressure_03", "reading": 1013.2, "location": "rack_C"},
        ]))
        assert inserted == 3
        rows = json.loads(db.query_json("SELECT * FROM data ORDER BY id"))
        assert len(rows) == 3
        reopened = DatabaseOps.open_with_s3(path, s3_config())
        reopened_rows = json.loads(reopened.query_json("SELECT COUNT(*) AS cnt FROM data"))
        assert reopened_rows[0]["cnt"] == 3
        results.ok("insert_json")
        results.ok("query_json")
        results.ok("persistence across reopen")
    except Exception as e:
        results.fail("insert/query/reopen", str(e))


def test_merge(results):
    section("Test 6: S3 MERGE")
    try:
        db = DatabaseOps.create_with_s3(unique_s3_path(), iot_schema(), s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 20.0, "location": "A"}
        ]))
        merge_result = json.loads(db.merge_json(
            json.dumps([
                {"id": 1, "sensor": "temp_01", "reading": 99.9, "location": "A_updated", "_op": "UPDATE"},
                {"id": 2, "sensor": "humidity_02", "reading": 50.0, "location": "B", "_op": "INSERT"},
            ]),
            "id",
        ))
        assert merge_result["rows_inserted"] >= 1
        assert merge_result["rows_updated"] >= 1
        rows = json.loads(db.query_json("SELECT * FROM data ORDER BY id"))
        assert len(rows) == 2
        assert rows[0]["reading"] == 99.9
        results.ok("merge_json")
        results.ok("merge persistence")
    except Exception as e:
        results.fail("merge_json", str(e))


def main():
    print("=" * 70)
    print("  posixlake Python S3 integration test")
    print("=" * 70)

    results = TestResult()

    if not minio_reachable():
        results.skip("MinIO dependency", "MinIO not reachable on localhost:9000")
        success = results.summary()
        sys.exit(0 if success else 1)

    test_create_with_s3(results)
    test_create_with_bad_endpoint(results)
    test_open_with_s3(results)
    test_health_check(results)
    test_insert_query_reopen(results)
    test_merge(results)

    success = results.summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
