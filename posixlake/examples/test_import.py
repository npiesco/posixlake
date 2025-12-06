#!/usr/bin/env python3
"""
Integration tests for posixlake Python bindings - CSV and Parquet import.

These are REAL integration tests that actually create databases, import data,
and verify the results. No mocks.
"""

import os
import sys
import tempfile
import shutil

from posixlake import DatabaseOps


def test_create_from_csv():
    """Test creating a database from CSV file with auto schema inference."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test CSV
        csv_path = os.path.join(tmpdir, 'data.csv')
        with open(csv_path, 'w') as f:
            f.write('id,name,score,active\n')
            f.write('1,Alice,95.5,true\n')
            f.write('2,Bob,87.3,false\n')
            f.write('3,Charlie,92.1,true\n')

        db_path = os.path.join(tmpdir, 'test_db')

        # Create database from CSV
        db = DatabaseOps.create_from_csv(db_path, csv_path)

        # Verify database was created
        assert os.path.exists(db_path), "Database path should exist"
        assert os.path.exists(os.path.join(db_path, '_delta_log')), "Delta log should exist"

        # Query the data
        result = db.query_json("SELECT * FROM data ORDER BY id")
        import json
        rows = json.loads(result)

        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        assert rows[0]['name'] == 'Alice'
        assert rows[1]['name'] == 'Bob'
        assert rows[2]['name'] == 'Charlie'

        # Verify schema inference
        schema = db.get_schema()
        field_types = {f.name: f.data_type for f in schema.fields}
        assert field_types['id'] == 'Int64', f"id should be Int64, got {field_types['id']}"
        assert field_types['score'] == 'Float64', f"score should be Float64, got {field_types['score']}"
        assert field_types['active'] == 'Boolean', f"active should be Boolean, got {field_types['active']}"
        assert field_types['name'] == 'String', f"name should be String, got {field_types['name']}"

        print("✓ test_create_from_csv PASSED")


def test_create_from_parquet():
    """Test creating a database from Parquet file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # First create a source database to get a parquet file
        csv_path = os.path.join(tmpdir, 'source.csv')
        with open(csv_path, 'w') as f:
            f.write('id,value\n')
            f.write('1,100\n')
            f.write('2,200\n')
            f.write('3,300\n')

        source_db_path = os.path.join(tmpdir, 'source_db')
        source_db = DatabaseOps.create_from_csv(source_db_path, csv_path)

        # Find the parquet file
        parquet_files = [f for f in os.listdir(source_db_path) if f.endswith('.parquet')]
        assert len(parquet_files) > 0, "Source DB should have parquet files"
        parquet_path = os.path.join(source_db_path, parquet_files[0])

        # Create new database from parquet
        db_path = os.path.join(tmpdir, 'parquet_db')
        db = DatabaseOps.create_from_parquet(db_path, parquet_path)

        # Verify database was created
        assert os.path.exists(db_path), "Database path should exist"
        assert os.path.exists(os.path.join(db_path, '_delta_log')), "Delta log should exist"

        # Query the data
        result = db.query_json("SELECT * FROM data ORDER BY id")
        import json
        rows = json.loads(result)

        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        assert rows[0]['id'] == 1
        assert rows[1]['id'] == 2
        assert rows[2]['id'] == 3

        print("✓ test_create_from_parquet PASSED")


def test_create_from_csv_large_file():
    """Test CSV import with larger dataset to verify batching works."""
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, 'large.csv')
        with open(csv_path, 'w') as f:
            f.write('id,name,value\n')
            for i in range(1000):
                f.write(f'{i},name_{i},{i * 10}\n')

        db_path = os.path.join(tmpdir, 'large_db')
        db = DatabaseOps.create_from_csv(db_path, csv_path)

        # Verify row count
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        import json
        rows = json.loads(result)
        assert rows[0]['cnt'] == 1000, f"Expected 1000 rows, got {rows[0]['cnt']}"

        print("✓ test_create_from_csv_large_file PASSED")


def test_create_from_csv_then_query_and_insert():
    """Test full workflow: import CSV, query, insert more data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, 'data.csv')
        with open(csv_path, 'w') as f:
            f.write('id,name\n')
            f.write('1,Alice\n')
            f.write('2,Bob\n')

        db_path = os.path.join(tmpdir, 'workflow_db')
        db = DatabaseOps.create_from_csv(db_path, csv_path)

        # Query initial data
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        import json
        rows = json.loads(result)
        assert rows[0]['cnt'] == 2

        # Insert more data
        db.insert_json('[{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}]')

        # Query again
        result = db.query_json("SELECT COUNT(*) as cnt FROM data")
        rows = json.loads(result)
        assert rows[0]['cnt'] == 4, f"Expected 4 rows after insert, got {rows[0]['cnt']}"

        print("✓ test_create_from_csv_then_query_and_insert PASSED")


if __name__ == '__main__':
    test_create_from_csv()
    test_create_from_parquet()
    test_create_from_csv_large_file()
    test_create_from_csv_then_query_and_insert()
    print("\n=== All Python import tests PASSED ===")
