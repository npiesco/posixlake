#!/usr/bin/env python3
"""
Integration tests for posixlake Python S3 bindings via UniFFI.

Requires MinIO running on localhost:9000 with bucket 'posixlake-test'.
Start MinIO:
    podman run -d --name posixlake-minio -p 9000:9000 -p 9001:9001 \
      minio/minio server /data --console-address ":9001"
    podman exec posixlake-minio mc alias set local http://localhost:9000 minioadmin minioadmin
    podman exec posixlake-minio mc mb local/posixlake-test

Run: uv run pytest tests/test_s3.py -v
"""

import json
import socket
import time

import pytest

from posixlake import (
    DatabaseOps,
    Field,
    HealthStatus,
    PosixLakeError,
    S3Config,
    Schema,
)

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "posixlake-test"
S3_REGION = "us-east-1"


def _minio_reachable() -> bool:
    """Check if MinIO is accepting connections on localhost:9000."""
    try:
        sock = socket.create_connection(("127.0.0.1", 9000), timeout=2)
        sock.close()
        return True
    except OSError:
        return False


requires_minio = pytest.mark.skipif(
    not _minio_reachable(),
    reason="MinIO not reachable on localhost:9000",
)


def _s3_config() -> S3Config:
    return S3Config(
        endpoint=S3_ENDPOINT,
        access_key_id=S3_ACCESS_KEY,
        secret_access_key=S3_SECRET_KEY,
        region=S3_REGION,
    )


def _unique_s3_path() -> str:
    """Generate a unique S3 path using timestamp to avoid test collisions."""
    ts = int(time.time() * 1000)
    return f"s3://{S3_BUCKET}/pytest_{ts}"


def _iot_schema() -> Schema:
    return Schema(
        fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="sensor", data_type="String", nullable=False),
            Field(name="reading", data_type="Float64", nullable=False),
            Field(name="location", data_type="String", nullable=True),
        ],
        primary_key="id",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_minio
class TestS3Create:
    """Test DatabaseOps.create_with_s3 via UniFFI."""

    def test_create_with_s3_returns_database(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        assert db is not None

    def test_create_with_s3_schema_matches(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        schema = db.get_schema()
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "sensor" in field_names
        assert "reading" in field_names
        assert "location" in field_names

    def test_create_with_bad_endpoint_fails(self):
        bad_config = S3Config(
            endpoint="http://localhost:19999",
            access_key_id=S3_ACCESS_KEY,
            secret_access_key=S3_SECRET_KEY,
            region=S3_REGION,
        )
        with pytest.raises(PosixLakeError):
            DatabaseOps.create_with_s3(_unique_s3_path(), _iot_schema(), bad_config)


@requires_minio
class TestS3Open:
    """Test DatabaseOps.open_with_s3 via UniFFI."""

    def test_open_existing_s3_database(self):
        s3_path = _unique_s3_path()
        db_create = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        # Insert a row so the table has data
        db_create.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"}
        ]))
        del db_create

        # Re-open the same S3 path
        db_open = DatabaseOps.open_with_s3(s3_path, _s3_config())
        rows = json.loads(db_open.query_json("SELECT * FROM data"))
        assert len(rows) == 1
        assert rows[0]["sensor"] == "temp_01"

    def test_open_nonexistent_s3_path_fails(self):
        with pytest.raises(PosixLakeError):
            DatabaseOps.open_with_s3(
                f"s3://{S3_BUCKET}/does_not_exist_{int(time.time())}",
                _s3_config(),
            )


@requires_minio
class TestS3HealthCheck:
    """Test health_check on an S3-backed database."""

    def test_health_check_returns_status(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        health = db.health_check()
        assert isinstance(health, HealthStatus)
        # S3-backed DBs report 'not_initialized' because the local cache
        # path has no _delta_log yet; the real data lives on S3.
        assert health.status in ("healthy", "not_initialized")

    def test_health_check_uptime_positive(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"},
            {"id": 2, "sensor": "humidity_02", "reading": 55.0, "location": "rack_B"},
        ]))
        health = db.health_check()
        assert health.uptime_seconds >= 0


@requires_minio
class TestS3InsertQuery:
    """Test insert + query round-trip on S3-backed database."""

    def test_insert_and_query_json(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        rows_inserted = db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 23.5, "location": "rack_A"},
            {"id": 2, "sensor": "humidity_02", "reading": 55.0, "location": "rack_B"},
            {"id": 3, "sensor": "pressure_03", "reading": 1013.2, "location": "rack_C"},
        ]))
        assert rows_inserted == 3

        result = json.loads(db.query_json("SELECT * FROM data ORDER BY id"))
        assert len(result) == 3
        assert result[0]["sensor"] == "temp_01"
        assert result[1]["reading"] == 55.0
        assert result[2]["location"] == "rack_C"

    def test_insert_query_reopen_persists(self):
        """Data survives close and reopen — proves S3 persistence."""
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        db.insert_json(json.dumps([
            {"id": 10, "sensor": "co2_04", "reading": 400.0, "location": "lab_01"},
        ]))
        del db

        db2 = DatabaseOps.open_with_s3(s3_path, _s3_config())
        result = json.loads(db2.query_json("SELECT sensor FROM data WHERE id = 10"))
        assert len(result) == 1
        assert result[0]["sensor"] == "co2_04"

    def test_multiple_inserts_accumulate(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 20.0, "location": "A"},
        ]))
        db.insert_json(json.dumps([
            {"id": 2, "sensor": "temp_02", "reading": 21.0, "location": "B"},
        ]))
        result = json.loads(db.query_json("SELECT COUNT(*) AS cnt FROM data"))
        assert result[0]["cnt"] == 2


@requires_minio
class TestS3Merge:
    """Test MERGE (upsert) on S3-backed database."""

    def test_merge_upsert(self):
        s3_path = _unique_s3_path()
        db = DatabaseOps.create_with_s3(s3_path, _iot_schema(), _s3_config())
        db.insert_json(json.dumps([
            {"id": 1, "sensor": "temp_01", "reading": 20.0, "location": "A"},
        ]))

        # merge_json requires an "_op" field: INSERT, UPDATE, DELETE
        merge_json_str = db.merge_json(
            json.dumps([
                {"id": 1, "sensor": "temp_01", "reading": 99.9, "location": "A_updated", "_op": "UPDATE"},
                {"id": 2, "sensor": "humidity_02", "reading": 50.0, "location": "B", "_op": "INSERT"},
            ]),
            "id",
        )
        merge_result = json.loads(merge_json_str)
        assert merge_result["rows_inserted"] >= 1
        assert merge_result["rows_updated"] >= 1

        rows = json.loads(db.query_json("SELECT * FROM data ORDER BY id"))
        assert len(rows) == 2
        assert rows[0]["reading"] == 99.9
        assert rows[0]["location"] == "A_updated"
        assert rows[1]["sensor"] == "humidity_02"
