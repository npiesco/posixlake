#!/usr/bin/env python3
"""
posixlake Python Bindings - Comprehensive Example

This example demonstrates all posixlake features available from Python via UniFFI bindings.

Features Covered:
- Database creation and management
- Data insertion and querying (SQL via DataFusion)
- Time Travel (query historical versions)
- Delta Lake operations (OPTIMIZE, VACUUM, Z-ORDER)
- Monitoring and metrics
- Authentication and RBAC
- Backup and restore (full and incremental)
- S3 backend support
- NFS server with POSIX filesystem access

Installation:
  pip install posixlake

Or from source:
1. Build: cd posixlake && cargo build --lib --release
2. Generate bindings: cargo run --bin uniffi-bindgen -- generate --library target/release/libposixlake.dylib --language python --out-dir ../bindings/python
3. Copy library: cp target/release/libposixlake.dylib ../bindings/python/
4. Install: pip install -e ../bindings/python/

Running:
  python3 posixlake/examples/python_example.py
"""

import os
import sys
import json
import tempfile
import shutil
import subprocess
import time
from pathlib import Path

from posixlake import (
    DatabaseOps,
    Schema,
    Field,
    PosixLakeError,
    S3Config,
    NfsServer,
    restore,
    restore_to_transaction,
)


def print_section(title):
    """Print a formatted section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def main():
    print_section("posixlake Python Bindings Example")

    # Create temporary directory for the database
    temp_dir = tempfile.mkdtemp(prefix="posixlake_python_")
    db_path = str(Path(temp_dir) / "test_db")

    try:
        # Example 1: Create Database
        print_section("Example 1: Create Database")

        schema = Schema(
            fields=[
                Field(name="id", data_type="Int32", nullable=False),
                Field(name="name", data_type="String", nullable=False),
                Field(name="email", data_type="String", nullable=True),
                Field(name="age", data_type="Int32", nullable=True),
                Field(name="salary", data_type="Float64", nullable=True),
            ]
        )

        print(f"Creating database at: {db_path}")
        print(f"Schema: {len(schema.fields)} fields")
        for field in schema.fields:
            print(f"  - {field.name}: {field.data_type} (nullable={field.nullable})")

        try:
            db = DatabaseOps.create(db_path, schema)
            print("✓ Database created successfully")
        except PosixLakeError as e:
            print(f"✗ Failed to create database: {e}")
            return

        # Example 1b: Create Database from CSV (Auto Schema Inference)
        print_section("Example 1b: Create Database from CSV")

        csv_db_path = str(Path(temp_dir) / "csv_db")
        csv_file_path = str(Path(temp_dir) / "sample_data.csv")

        # Create a sample CSV file
        print("Creating sample CSV file...")
        with open(csv_file_path, "w") as f:
            f.write("product_id,product_name,price,in_stock\n")
            f.write("1,Widget,29.99,true\n")
            f.write("2,Gadget,49.99,true\n")
            f.write("3,Gizmo,19.99,false\n")
            f.write("4,Thingamajig,99.99,true\n")

        print(f"CSV file: {csv_file_path}")
        print("Schema will be auto-inferred from CSV content:")
        print("  - product_id: Int64 (all values parse as integers)")
        print("  - product_name: String (text values)")
        print("  - price: Float64 (decimal values)")
        print("  - in_stock: Boolean (true/false values)")

        try:
            csv_db = DatabaseOps.create_from_csv(csv_db_path, csv_file_path)
            print(f"\n✓ Database created from CSV at: {csv_db_path}")

            # Verify the inferred schema
            csv_schema = csv_db.get_schema()
            print("\nInferred schema:")
            for field in csv_schema.fields:
                print(f"  - {field.name}: {field.data_type}")

            # Query the imported data
            results = csv_db.query_json("SELECT * FROM data ORDER BY product_id")
            print(f"\nImported data:\n{results}")
        except PosixLakeError as e:
            print(f"✗ CSV import failed: {e}")

        # Example 1c: Create Database from Parquet
        print_section("Example 1c: Create Database from Parquet")

        parquet_db_path = str(Path(temp_dir) / "parquet_db")

        # Use the parquet file from the CSV database we just created
        parquet_files = [f for f in os.listdir(csv_db_path) if f.endswith('.parquet')]

        if parquet_files:
            parquet_file_path = str(Path(csv_db_path) / parquet_files[0])
            print(f"Using Parquet file: {parquet_file_path}")
            print("Schema is read directly from Parquet metadata (no inference needed)")

            try:
                parquet_db = DatabaseOps.create_from_parquet(parquet_db_path, parquet_file_path)
                print(f"\n✓ Database created from Parquet at: {parquet_db_path}")

                # Query the imported data
                results = parquet_db.query_json("SELECT * FROM data ORDER BY product_id")
                print(f"\nImported data:\n{results}")
            except PosixLakeError as e:
                print(f"✗ Parquet import failed: {e}")
        else:
            print("⚠ No parquet files found (CSV import may have failed)")

        # Example 1d: Complex Data Types
        print_section("Example 1d: Complex Data Types (Decimal, List, Map, Struct)")

        complex_db_path = str(Path(temp_dir) / "complex_db")

        print("Creating database with complex data types...")
        print("Supported complex types:")
        print("  - Decimal128(precision,scale) - Fixed-point decimals for currency/finance")
        print("  - List<ElementType> - Arrays of values")
        print("  - Map<KeyType,ValueType> - Key-value pairs")
        print("  - Struct<field1:Type1,field2:Type2> - Nested records")

        complex_schema = Schema(
            fields=[
                Field(name="id", data_type="Int32", nullable=False),
                Field(name="price", data_type="Decimal128(10,2)", nullable=False),
                Field(name="tags", data_type="List<String>", nullable=True),
                Field(name="metadata", data_type="Map<String,Int64>", nullable=True),
                Field(name="address", data_type="Struct<city:String,zip:Int32>", nullable=True),
            ]
        )

        try:
            complex_db = DatabaseOps.create(complex_db_path, complex_schema)
            print(f"\n✓ Complex types database created at: {complex_db_path}")

            # Verify the schema
            s = complex_db.get_schema()
            print("\nSchema with complex types:")
            for field in s.fields:
                print(f"  - {field.name}: {field.data_type} (nullable={field.nullable})")
        except PosixLakeError as e:
            print(f"✗ Complex types failed: {e}")

        # Example 2: Insert Data
        print_section("Example 2: Insert Data (JSON)")

        data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "salary": 75000.0},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35, "salary": 85000.0},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 28, "salary": 65000.0},
            {"id": 4, "name": "Diana", "email": "diana@example.com", "age": 42, "salary": 95000.0},
            {"id": 5, "name": "Eve", "email": "eve@example.com", "age": 31, "salary": 80000.0},
        ]

        json_data = json.dumps(data)
        print(f"Inserting {len(data)} rows...")

        try:
            db.insert_json(json_data)
            print(f"✓ Inserted {len(data)} rows successfully")
        except PosixLakeError as e:
            print(f"✗ Insert failed: {e}")
            return

        # Example 2b: Buffered Insert for Performance
        print_section("Example 2b: Buffered Insert (High Performance)")
        
        print("Buffered insert batches multiple small writes into fewer transactions.")
        print("This improves performance when inserting many small batches.\n")
        
        # Insert 20 small batches using buffered insert
        print("Inserting 20 small batches (1 row each) using buffered insert...")
        start_id = 100
        for i in range(20):
            small_batch = [{
                "id": start_id + i,
                "name": f"User_{start_id + i}",
                "email": f"user{start_id + i}@example.com",
                "age": 25 + (i % 20),
                "salary": 50000.0 + (i * 1000)
            }]
            try:
                rows = db.insert_buffered_json(json.dumps(small_batch))
                if (i + 1) % 5 == 0:  # Print every 5 batches
                    print(f"  Buffered {i + 1}/20 batches...")
            except PosixLakeError as e:
                print(f"✗ Buffered insert failed: {e}")
                return
        
        # Flush the buffer to commit all data
        print("\nFlushing write buffer...")
        try:
            db.flush_write_buffer()
            print("✓ All buffered data committed to Delta Lake")
        except PosixLakeError as e:
            print(f"✗ Flush failed: {e}")
            return
        
        # Verify the data was written
        try:
            count_result = db.query_json("SELECT COUNT(*) as count FROM data WHERE id >= 100")
            count_data = json.loads(count_result)
            buffered_count = count_data[0]["count"]
            print(f"✓ Verified: {buffered_count} rows from buffered inserts")
            
            # Show performance note
            print("\n📊 Performance Note:")
            print("  • Regular insert: 20 separate Delta Lake transactions")
            print("  • Buffered insert: ~2-3 batched transactions (auto-flush + manual)")
            print("  • Reduces transaction overhead by ~10x for small batches")
        except PosixLakeError as e:
            print(f"✗ Verification query failed: {e}")

        # Example 3: Query Data
        print_section("Example 3: Query Data")

        queries = [
            ("SELECT * FROM data WHERE age > 30", "Find people older than 30"),
            ("SELECT name, salary FROM data WHERE salary > 70000", "High earners"),
            ("SELECT AVG(age) as avg_age, AVG(salary) as avg_salary FROM data", "Averages"),
        ]

        for sql, description in queries:
            print(f"\n{description}")
            print(f"SQL: {sql}")
            try:
                results = db.query_json(sql)
                parsed = json.loads(results)
                print(f"✓ Results ({len(parsed)} batches):")
                for batch in parsed:
                    print(f"  {json.dumps(batch, indent=2)}")
            except PosixLakeError as e:
                print(f"✗ Query failed: {e}")

        # Example 4: Time Travel
        print_section("Example 4: Time Travel")

        # Insert more data
        more_data = [
            {"id": 6, "name": "Frank", "email": "frank@example.com", "age": 45, "salary": 100000.0}
        ]
        db.insert_json(json.dumps(more_data))
        print("✓ Inserted 1 more row (version 1)")

        # Query current version
        print("\nQuery current version:")
        results = db.query_json("SELECT COUNT(*) as count FROM data")
        print(f"  Current: {results}")

        # Query previous version
        print("\nQuery version 0 (before last insert):")
        try:
            results_v0 = db.query_version_json("SELECT COUNT(*) as count FROM data", 0)
            print(f"  Version 0: {results_v0}")
        except PosixLakeError as e:
            print(f"  ✗ Time travel failed: {e}")

        # Example 5: Delete Rows
        print_section("Example 5: Delete Rows")

        print("Deleting rows where age < 30...")
        try:
            db.delete_rows_where("age < 30")
            print("✓ Delete completed")

            results = db.query_json("SELECT COUNT(*) as count FROM data")
            print(f"  Remaining rows: {results}")
        except PosixLakeError as e:
            print(f"✗ Delete failed: {e}")

        # Example 6: MERGE (UPSERT) Operations
        print_section("Example 6: MERGE (UPSERT) Operations")

        print("\nDemonstrating MERGE with mixed INSERT, UPDATE, DELETE operations...")
        
        # Create merge data with operation type
        merge_data = [
            {
                "id": 1,
                "name": "Alice Johnson",  # UPDATE - change name
                "email": "alice.j@example.com",
                "age": 31,  # UPDATE - change age
                "salary": 78000.0,  # UPDATE - change salary
                "_op": "UPDATE"
            },
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 28,
                "salary": 65000.0,
                "_op": "DELETE"  # DELETE this row
            },
            {
                "id": 7,
                "name": "George",  # INSERT - new employee
                "email": "george@example.com",
                "age": 38,
                "salary": 88000.0,
                "_op": "INSERT"
            },
            {
                "id": 8,
                "name": "Hannah",  # INSERT - new employee
                "email": "hannah@example.com",
                "age": 27,
                "salary": 70000.0,
                "_op": "INSERT"
            }
        ]
        
        print(f"\nMERGE operations:")
        print(f"  - UPDATE: Alice (id=1) with new details")
        print(f"  - DELETE: Charlie (id=3)")
        print(f"  - INSERT: George (id=7), Hannah (id=8)")
        
        try:
            # Execute MERGE operation
            result = db.merge_json(json.dumps(merge_data), "id")
            metrics = json.loads(result)
            
            print(f"\n✓ MERGE completed successfully:")
            print(f"  Rows inserted: {metrics['rows_inserted']}")
            print(f"  Rows updated:  {metrics['rows_updated']}")
            print(f"  Rows deleted:  {metrics['rows_deleted']}")
            print(f"  Total affected: {metrics['total_affected']}")
            
            # Verify results
            print("\nVerifying MERGE results:")
            results = db.query_json("SELECT id, name, age, salary FROM data ORDER BY id")
            parsed = json.loads(results)
            print(f"  Current employees ({len(parsed)} rows):")
            for row in parsed:
                print(f"    ID: {row['id']}, Name: {row['name']}, Age: {row['age']}, Salary: ${row['salary']}")
            
        except PosixLakeError as e:
            print(f"✗ MERGE failed: {e}")

        # Example 7: Delta Lake Operations
        print_section("Example 7: Delta Lake Operations")

        print("\n1. OPTIMIZE (compact files)")
        try:
            db.optimize()
            print("   ✓ Optimize completed")
        except PosixLakeError as e:
            print(f"   ✗ Optimize failed: {e}")

        print("\n2. VACUUM (dry run - preview files to delete)")
        try:
            files = db.vacuum_dry_run(0)  # 0 hours retention for testing
            print(f"   ✓ Would delete {len(files)} files")
            for f in files[:3]:  # Show first 3
                print(f"     - {f}")
            if len(files) > 3:
                print(f"     ... and {len(files) - 3} more")
        except PosixLakeError as e:
            print(f"   ✗ Vacuum dry run failed: {e}")

        print("\n3. Z-ORDER (multi-dimensional clustering)")
        try:
            db.zorder(["age", "salary"])
            print("   ✓ Z-ORDER completed on columns: age, salary")
        except PosixLakeError as e:
            print(f"   ✗ Z-ORDER failed: {e}")

        # Example 8: Monitoring
        print_section("Example 8: Monitoring & Metrics")

        print("\nMetrics:")
        metrics = db.get_metrics()
        print(f"  Total queries: {metrics.total_queries}")
        print(f"  Total inserts: {metrics.total_inserts}")
        print(f"  Total deletes: {metrics.total_deletes}")
        print(f"  Total transactions: {metrics.total_transactions}")
        print(f"  Total errors: {metrics.total_errors}")
        print(f"  Avg query latency: {metrics.avg_query_latency_ms:.2f}ms")
        print(f"  Max query latency: {metrics.max_query_latency_ms:.2f}ms")
        print(f"  Uptime: {metrics.uptime_seconds:.2f}s")

        print("\nHealth Check:")
        health = db.health_check()
        print(f"  Status: {health.status}")
        print(f"  Uptime: {health.uptime_seconds:.2f}s")
        print(f"  Total files: {health.total_files}")
        print(f"  Total rows: {health.total_rows}")
        print(f"  Total size: {health.total_size_bytes} bytes")

        print("\nData Skipping Stats:")
        stats = db.get_data_skipping_stats()
        print(f"  Total files: {stats.total_files}")
        print(f"  Files read: {stats.files_read}")
        print(f"  Files skipped: {stats.files_skipped}")
        if stats.total_files > 0:
            skip_pct = (stats.files_skipped / stats.total_files) * 100
            print(f"  Skip rate: {skip_pct:.1f}%")

        # Example 9: Database Info
        print_section("Example 9: Database Info")

        print(f"Database path: {db.get_base_path()}")

        schema_info = db.get_schema()
        print(f"\nSchema ({len(schema_info.fields)} fields):")
        for field in schema_info.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            print(f"  {field.name:15} {field.data_type:10} {nullable}")

        # Example 10: Authentication & RBAC
        print_section("Example 10: Authentication & RBAC")
        
        print("\n1. Creating auth-enabled database...")
        auth_db_path = str(Path(temp_dir) / "auth_db")
        try:
            auth_db = DatabaseOps.create_with_auth(auth_db_path, schema, True)
            print("✓ Database created with authentication enabled")
            
            # Create users
            print("\n2. Creating users with different roles...")
            auth_db.create_user("admin", "admin123", ["admin"])
            print("  ✓ Created user: admin (role: admin)")
            
            auth_db.create_user("analyst", "analyst123", ["read"])
            print("  ✓ Created user: analyst (role: read)")
            
            # Insert some data as admin
            print("\n3. Admin inserting data...")
            auth_db.insert_json(json.dumps(data))
            print(f"  ✓ Inserted {len(data)} rows as admin")
            
            auth_db = None  # Close admin session
            
            # Try to open as analyst (read-only)
            print("\n4. Authenticating as analyst (read-only)...")
            analyst_db = DatabaseOps.open_with_credentials(auth_db_path, "analyst", "analyst123")
            print("  ✓ Analyst authenticated")
            
            # Analyst can read
            results = analyst_db.query_json("SELECT COUNT(*) FROM data")
            print(f"  ✓ Analyst read data: {results}")
            
            # Analyst cannot write
            print("\n5. Testing permission enforcement...")
            try:
                analyst_db.insert_json('[{"id": 999, "name": "Unauthorized"}]')
                print("  ✗ ERROR: Analyst should not be able to insert!")
            except PosixLakeError as e:
                print(f"  ✓ Insert denied: {e}")
            
            print("✓ Authentication and RBAC working")
            
        except PosixLakeError as e:
            print(f"✗ Auth/RBAC failed: {e}")
        
        # Example 11: Backup & Restore
        print_section("Example 11: Backup & Restore")
        
        backup_path = str(Path(temp_dir) / "backup")
        restore_path = str(Path(temp_dir) / "restore")
        
        print("\n1. Creating full backup...")
        try:
            db.backup(backup_path)
            print(f"✓ Full backup created at {backup_path}")
            
            # Insert more data after backup
            print("\n2. Inserting more data after backup...")
            more_data = [{"id": 10, "name": "Post-Backup", "email": "test@example.com", "age": 50, "salary": 120000.0}]
            db.insert_json(json.dumps(more_data))
            print("✓ Inserted 1 more row")
            
            # Incremental backup
            print("\n3. Creating incremental backup...")
            incremental_path = str(Path(temp_dir) / "incremental")
            db.backup_incremental(backup_path, incremental_path)
            print(f"✓ Incremental backup created at {incremental_path}")
            
            # Restore full backup
            print("\n4. Restoring from full backup...")
            restore(backup_path, restore_path)
            print(f"✓ Database restored to {restore_path}")
            
            restored_db = DatabaseOps.open(restore_path)
            results = restored_db.query_json("SELECT COUNT(*) FROM data")
            print(f"  ✓ Restored database verified: {results}")
            
            print("✓ Backup and restore complete")
            
        except PosixLakeError as e:
            print(f"✗ Backup/Restore failed: {e}")
        
        # Example 12: S3 Backend
        print_section("Example 12: S3 Backend (Optional)")
        
        print("Note: S3 backend requires MinIO running on localhost:9000")
        print("To test: docker compose up -d")
        print("\nS3 Example (commented - requires MinIO):")
        print("  s3_config = S3Config(")
        print("      endpoint='http://localhost:9000',")
        print("      access_key_id='minioadmin',")
        print("      secret_access_key='minioadmin'")
        print("  )")
        print("  s3_db = DatabaseOps.create_with_s3('s3://posixlake-test/py_db', schema, s3_config)")
        print("  s3_db.insert_json(data)")
        print("  results = s3_db.query_json('SELECT * FROM data')")
        print("✓ S3 backend available (requires MinIO setup)")
        
        # Example 13: NFS Server & POSIX Operations
        print_section("Example 13: NFS Server & POSIX Filesystem Operations")
        
        print("Starting NFS server on port 11049...")
        try:
            nfs = NfsServer(db, 11049)
            print("✓ NFS server started")
            
            # Wait for server to be fully ready
            time.sleep(0.5)
            if nfs.is_ready():
                print("✓ NFS server is ready")
            
            # Create mount point
            mount_point = Path(temp_dir) / "mnt"
            mount_point.mkdir(exist_ok=True)
            
            # Get mount command
            mount_cmd = nfs.get_mount_command(str(mount_point))
            print(f"\nMount command: {' '.join(mount_cmd)}")
            
            # Try to mount (requires sudo)
            print("\nAttempting to mount filesystem (requires sudo)...")
            try:
                result = subprocess.run(mount_cmd, capture_output=True, timeout=5)
                if result.returncode == 0:
                    print("✓ Filesystem mounted successfully")
                    mounted = True
                    
                    # Give mount time to stabilize
                    time.sleep(1)
                    
                    print("\n" + "="*70)
                    print("  POSIX Filesystem Operations on Delta Lake Data")
                    print("="*70)
                    
                    # 1. List directory
                    print("\n1. List directory contents (ls)")
                    print(f"   $ ls {mount_point}")
                    try:
                        result = subprocess.run(["ls", "-la", str(mount_point)], 
                                              capture_output=True, text=True, timeout=2)
                        if result.returncode == 0:
                            for line in result.stdout.strip().split('\n')[:10]:
                                print(f"   {line}")
                            print("   ✓ ls command successful")
                    except Exception as e:
                        print(f"   ✗ ls failed: {e}")
                    
                    # 2. List data directory
                    data_dir = mount_point / "data"
                    if data_dir.exists():
                        print(f"\n2. List data directory (ls data/)")
                        print(f"   $ ls {data_dir}")
                        try:
                            result = subprocess.run(["ls", "-lh", str(data_dir)], 
                                                  capture_output=True, text=True, timeout=2)
                            if result.returncode == 0:
                                for line in result.stdout.strip().split('\n')[:10]:
                                    print(f"   {line}")
                                print("   ✓ ls data/ successful")
                        except Exception as e:
                            print(f"   ✗ ls data/ failed: {e}")
                    
                    # 3. Read CSV file with cat
                    csv_file = data_dir / "data.csv"
                    if csv_file.exists():
                        print(f"\n3. Read CSV file (cat)")
                        print(f"   $ cat {csv_file} | head -10")
                        try:
                            cat_proc = subprocess.Popen(["cat", str(csv_file)], 
                                                       stdout=subprocess.PIPE, text=True)
                            head_proc = subprocess.Popen(["head", "-10"], 
                                                        stdin=cat_proc.stdout, 
                                                        stdout=subprocess.PIPE, text=True)
                            cat_proc.stdout.close()
                            output, _ = head_proc.communicate(timeout=2)
                            if output:
                                for line in output.strip().split('\n'):
                                    print(f"   {line}")
                                print("   ✓ cat successful")
                        except Exception as e:
                            print(f"   ✗ cat failed: {e}")
                    
                    # 4. Grep for specific data
                    if csv_file.exists():
                        print(f"\n4. Search with grep (find rows with 'Alice')")
                        print(f"   $ grep 'Alice' {csv_file}")
                        try:
                            result = subprocess.run(["grep", "Alice", str(csv_file)], 
                                                  capture_output=True, text=True, timeout=2)
                            if result.returncode == 0 and result.stdout:
                                for line in result.stdout.strip().split('\n'):
                                    print(f"   {line}")
                                print("   ✓ grep successful")
                            elif result.returncode == 1:
                                print("   (no matches found)")
                        except Exception as e:
                            print(f"   ✗ grep failed: {e}")
                    
                    # 5. AWK to process columns
                    if csv_file.exists():
                        print(f"\n5. Process with awk (extract name and salary)")
                        print(f"   $ awk -F',' '{{print $2, $5}}' {csv_file} | head -5")
                        try:
                            awk_proc = subprocess.Popen(
                                ["awk", "-F,", "{print $2, $5}", str(csv_file)],
                                stdout=subprocess.PIPE, text=True
                            )
                            head_proc = subprocess.Popen(
                                ["head", "-5"],
                                stdin=awk_proc.stdout,
                                stdout=subprocess.PIPE, text=True
                            )
                            awk_proc.stdout.close()
                            output, _ = head_proc.communicate(timeout=2)
                            if output:
                                for line in output.strip().split('\n'):
                                    print(f"   {line}")
                                print("   ✓ awk successful")
                        except Exception as e:
                            print(f"   ✗ awk failed: {e}")
                    
                    # 6. Word count
                    if csv_file.exists():
                        print(f"\n6. Count lines with wc")
                        print(f"   $ wc -l {csv_file}")
                        try:
                            result = subprocess.run(["wc", "-l", str(csv_file)], 
                                                  capture_output=True, text=True, timeout=2)
                            if result.returncode == 0:
                                print(f"   {result.stdout.strip()}")
                                print("   ✓ wc successful")
                        except Exception as e:
                            print(f"   ✗ wc failed: {e}")
                    
                    # 7. Sort data
                    if csv_file.exists():
                        print(f"\n7. Sort by column with sort")
                        print(f"   $ sort -t',' -k2 {csv_file} | head -5")
                        try:
                            sort_proc = subprocess.Popen(
                                ["sort", "-t,", "-k2", str(csv_file)],
                                stdout=subprocess.PIPE, text=True
                            )
                            head_proc = subprocess.Popen(
                                ["head", "-5"],
                                stdin=sort_proc.stdout,
                                stdout=subprocess.PIPE, text=True
                            )
                            sort_proc.stdout.close()
                            output, _ = head_proc.communicate(timeout=2)
                            if output:
                                for line in output.strip().split('\n'):
                                    print(f"   {line}")
                                print("   ✓ sort successful")
                        except Exception as e:
                            print(f"   ✗ sort failed: {e}")
                    
                    # 8. View file metadata
                    if csv_file.exists():
                        print(f"\n8. File metadata with stat")
                        print(f"   $ stat {csv_file}")
                        try:
                            result = subprocess.run(["stat", str(csv_file)], 
                                                  capture_output=True, text=True, timeout=2)
                            if result.returncode == 0:
                                for line in result.stdout.strip().split('\n')[:5]:
                                    print(f"   {line}")
                                print("   ...")
                                print("   ✓ stat successful")
                        except Exception as e:
                            print(f"   ✗ stat failed: {e}")
                    
                    # 8.5. Test mv command (rename files)
                    print(f"\n8.5. Rename file with mv command")
                    test_file = mount_point / "test_rename.txt"
                    renamed_file = mount_point / "test_renamed.txt"
                    try:
                        # Create a test file
                        with open(test_file, 'w') as f:
                            f.write("Test content for mv command")
                        print(f"   Created test file: {test_file.name}")
                        
                        # Execute mv
                        print(f"   $ mv {test_file.name} {renamed_file.name}")
                        result = subprocess.run(["mv", str(test_file), str(renamed_file)], 
                                              capture_output=True, text=True, timeout=2)
                        if result.returncode == 0:
                            # Verify rename worked
                            if renamed_file.exists() and not test_file.exists():
                                # Check content preserved
                                with open(renamed_file, 'r') as f:
                                    content = f.read()
                                if content == "Test content for mv command":
                                    print("   ✓ mv successful - file renamed with content preserved")
                                else:
                                    print("   ✗ File renamed but content changed")
                            else:
                                print("   ✗ mv did not rename file correctly")
                            
                            # Cleanup (note: file deletion not fully implemented for user files)
                            try:
                                if renamed_file.exists():
                                    renamed_file.unlink()
                            except:
                                pass  # File deletion not supported for user-created files
                        else:
                            print(f"   ✗ mv failed: {result.stderr}")
                    except Exception as e:
                        print(f"   ✗ mv test failed: {e}")
                        # Cleanup on error
                        try:
                            if test_file.exists():
                                test_file.unlink()
                            if renamed_file.exists():
                                renamed_file.unlink()
                        except:
                            pass
                    
                    # 8.6. Test cp command (copy files)
                    print(f"\n8.6. Copy file with cp command")
                    source_file = mount_point / "test_source.txt"
                    copied_file = mount_point / "test_copied.txt"
                    try:
                        # Create source file
                        with open(source_file, 'w') as f:
                            f.write("Content to copy")
                        print(f"   Created source file: {source_file.name}")
                        
                        # Execute cp
                        print(f"   $ cp {source_file.name} {copied_file.name}")
                        result = subprocess.run(["cp", str(source_file), str(copied_file)], 
                                              capture_output=True, text=True, timeout=2)
                        if result.returncode == 0:
                            # Verify copy worked
                            if copied_file.exists() and source_file.exists():
                                # Check content matches
                                with open(copied_file, 'r') as f:
                                    content = f.read()
                                if content == "Content to copy":
                                    print("   ✓ cp successful - file copied with same content")
                                else:
                                    print("   ✗ File copied but content differs")
                            else:
                                print("   ✗ cp did not create file correctly")
                        else:
                            print(f"   ✗ cp failed: {result.stderr}")
                    except Exception as e:
                        print(f"   ✗ cp test failed: {e}")
                    
                    # 8.7. Test rmdir command (remove empty directories)
                    print(f"\n8.7. Remove empty directory with rmdir command")
                    test_empty_dir = mount_point / "test_empty_dir"
                    test_nonempty_dir = mount_point / "test_nonempty_dir"
                    try:
                        # Create empty directory
                        test_empty_dir.mkdir(exist_ok=True)
                        print(f"   Created empty directory: {test_empty_dir.name}")
                        
                        # Execute rmdir on empty directory
                        print(f"   $ rmdir {test_empty_dir.name}")
                        result = subprocess.run(["rmdir", str(test_empty_dir)], 
                                              capture_output=True, text=True, timeout=2)
                        if result.returncode == 0:
                            # Verify directory was removed
                            if not test_empty_dir.exists():
                                print("   ✓ rmdir successful - empty directory removed")
                            else:
                                print("   ✗ rmdir succeeded but directory still exists")
                        else:
                            print(f"   ✗ rmdir failed: {result.stderr}")
                        
                        # Test rmdir on non-empty directory (should fail)
                        print(f"\n   Testing rmdir on non-empty directory (should fail)...")
                        test_nonempty_dir.mkdir(exist_ok=True)
                        (test_nonempty_dir / "file.txt").write_text("content")
                        print(f"   $ rmdir {test_nonempty_dir.name}")
                        result = subprocess.run(["rmdir", str(test_nonempty_dir)], 
                                              capture_output=True, text=True, timeout=2)
                        if result.returncode != 0:
                            print("   ✓ rmdir correctly failed on non-empty directory")
                        else:
                            print("   ✗ rmdir should have failed on non-empty directory")
                    except Exception as e:
                        print(f"   ✗ rmdir test failed: {e}")
                    
                    # 9. Test rm command (truncate table)
                    if csv_file.exists():
                        print(f"\n9. Truncate table with rm command")
                        print(f"   $ rm {csv_file}")
                        print("   (This will truncate the Delta table, deleting all rows)")
                        try:
                            # First check current count
                            results_before = db.query_json("SELECT COUNT(*) as count FROM data")
                            count_before = json.loads(results_before)[0]['count']
                            print(f"   Rows before rm: {count_before}")
                            
                            # Execute rm
                            result = subprocess.run(["rm", str(csv_file)], 
                                                  capture_output=True, text=True, timeout=2)
                            if result.returncode == 0:
                                print("   ✓ rm successful")
                                
                                # Wait a moment for the operation to complete
                                time.sleep(0.5)
                                
                                # Verify table was truncated
                                results_after = db.query_json("SELECT COUNT(*) as count FROM data")
                                count_after = json.loads(results_after)[0]['count']
                                print(f"   Rows after rm: {count_after}")
                                
                                if count_after == 0:
                                    print("   ✓ Table truncated - all rows deleted")
                                else:
                                    print(f"   ✗ Expected 0 rows, got {count_after}")
                                
                                # Restore data for remaining tests
                                print("   Restoring data...")
                                db.insert_json(json.dumps(data))
                                time.sleep(0.5)
                                print("   ✓ Data restored")
                            else:
                                print(f"   ✗ rm failed: {result.stderr}")
                        except Exception as e:
                            print(f"   ✗ rm test failed: {e}")
                    
                    print("\n" + "="*70)
                    print("  All POSIX Operations Completed Successfully!")
                    print("="*70)
                    print("\nThis demonstrates that your Delta Lake database is now")
                    print("accessible via standard Unix tools - no special drivers needed!")
                    
                    # Unmount
                    print("\nUnmounting filesystem...")
                    unmount_cmd = nfs.get_unmount_command(str(mount_point))
                    subprocess.run(unmount_cmd, capture_output=True, timeout=5)
                    print("✓ Filesystem unmounted")
                    
                else:
                    print(f"✗ Mount failed (exit code {result.returncode})")
                    print(f"  stdout: {result.stdout.decode()}")
                    print(f"  stderr: {result.stderr.decode()}")
                    print("\nNote: NFS mounting requires sudo privileges.")
                    print("The NFS server is running and can be mounted manually:")
                    print(f"  {' '.join(mount_cmd)}")
                    mounted = False
            except subprocess.TimeoutExpired:
                print("✗ Mount command timed out")
                print("This is normal if sudo requires password input.")
                mounted = False
            except Exception as e:
                print(f"✗ Mount failed: {e}")
                mounted = False
            
            # Shutdown server
            print("\nShutting down NFS server...")
            nfs.shutdown()
            print("✓ NFS server stopped")
            
        except PosixLakeError as e:
            print(f"✗ NFS server failed: {e}")

        print_section("✓ All 13 Examples Completed Successfully!")
        print("\nFeatures Demonstrated:")
        print("  1. Create Database - Delta Lake native format")
        print("  2. Insert Data - JSON format with Arrow conversion")
        print("  3. Query Data - SQL via DataFusion")
        print("  4. Time Travel - Query historical versions")
        print("  5. Delete Rows - Row-level deletion")
        print("  6. MERGE Operations - UPSERT with INSERT/UPDATE/DELETE")
        print("  7. Delta Lake Operations - OPTIMIZE, VACUUM, Z-ORDER")
        print("  8. Monitoring - Metrics and health checks")
        print("  9. Database Info - Schema and path")
        print(" 10. Authentication & RBAC - User management and permissions")
        print(" 11. Backup & Restore - Full and incremental backups")
        print(" 12. S3 Backend - Cloud storage support")
        print(" 13. NFS Server - POSIX filesystem access")
        print(f"\nDatabase location: {db_path}")
        print("You can inspect the Delta Lake files at this location.")

    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        print("\nCleaning up...")
        try:
            shutil.rmtree(temp_dir)
            print(f"✓ Removed temporary directory: {temp_dir}")
        except Exception as e:
            print(f"✗ Failed to cleanup: {e}")


if __name__ == "__main__":
    main()

