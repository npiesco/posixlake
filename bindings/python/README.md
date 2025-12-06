<div align="center">
  <h1>posixlake Python Bindings</h1>
  <p><strong>High-performance Delta Lake database with Python API and POSIX interface</strong></p>
  
  <p><em>Python API for posixlake (File Store Database) - Access Delta Lake operations, SQL queries, time travel, and use Unix commands (`cat`, `grep`, `awk`, `wc`, `head`, `tail`, `sort`, `cut`, `echo >>`, `sed -i`, `vim`, `mkdir`, `mv`, `cp`, `rmdir`, `rm`) to query and trigger Delta Lake transactions. Mount databases as POSIX filesystems where standard Unix tools execute ACID operations. Works with local filesystem directories and object storage/S3. Built on Rust for maximum performance.</em></p>

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![PyPI](https://img.shields.io/badge/PyPI-posixlake-3776AB?logo=pypi&logoColor=white)](https://pypi.org/project/posixlake/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Native%20Format-00ADD8?logo=delta&logoColor=white)](https://delta.io)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../LICENSE.md)
[![Rust](https://img.shields.io/badge/Powered%20by-Rust-orange.svg)](https://www.rust-lang.org)

[![Arrow](https://img.shields.io/badge/Arrow-56.2-red?logo=apache)](https://arrow.apache.org)
[![DataFusion](https://img.shields.io/badge/DataFusion-50.3-purple?logo=apache)](https://datafusion.apache.org)
[![S3 Compatible](https://img.shields.io/badge/S3-Compatible-569A31?logo=amazons3&logoColor=white)](.)
[![NFS Server](https://img.shields.io/badge/NFS-Pure%20Rust-orange)](.)
</div>

---

**Key Features:**
- **Delta Lake Native**: Full ACID transactions with native `_delta_log/` format
- **SQL Queries**: DataFusion-powered SQL engine embedded in Python
- **Time Travel**: Query historical versions and timestamps
- **CSV/Parquet Import**: Create databases from CSV (auto schema inference) or Parquet files
- **Buffered Inserts**: 10x performance improvement for small batch writes
- **NFS Server**: Mount Delta Lake as POSIX filesystem - standard Unix tools work directly
- **Storage Backends**: Works with local filesystem and S3/MinIO - same unified API
- **Performance**: Rust-powered engine with buffered inserts (~10x faster for small batches)
- **No Special Drivers**: Uses OS built-in NFS client - zero installation
- **Delta Lake Compatible**: Tables readable by Spark, Databricks, and Athena immediately

---

## Installation

### From PyPI (Recommended)

```bash
pip install posixlake
```

**Requirements:**
- **Python 3.11+** (required for prebuilt wheels with native library)
- For other Python versions, install from source (see below)

**PyPI Package:** https://pypi.org/project/posixlake/

### From Source

```bash
# 1. Clone the repository
git clone https://github.com/npiesco/posixlake.git
cd posixlake

# 2. Build Rust library
cargo build --release

# 3. Generate Python API
cargo run --bin uniffi-bindgen -- generate \
    --library target/release/libposixlake.dylib \
    --language python \
    --out-dir bindings/python

# 4. Copy library
cp target/release/libposixlake.dylib bindings/python/

# 5. Install Python package
cd bindings/python
pip install -e .
```

**Prerequisites:**
- Python 3.8+ (3.11+ recommended for prebuilt wheels)
- Rust 1.70+ (for building from source)
- NFS client (built-in on macOS/Linux/Windows Pro)

---

## Quick Start

### Example 1: Basic Database Operations

```python
from posixlake import DatabaseOps, Schema, Field, PosixLakeError

# Create a schema
schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
    Field(name="age", data_type="Int32", nullable=True),
    Field(name="salary", data_type="Float64", nullable=True),
])

# Create database on local filesystem
try:
    db = DatabaseOps.create("/path/to/db", schema)
    print("✓ Database created")
except PosixLakeError as e:
    print(f"✗ Error: {e}")

# Insert data (JSON format)
data = '[{"id": 1, "name": "Alice", "age": 30, "salary": 75000.0}]'
db.insert_json(data)

# Query with SQL
results = db.query_json("SELECT * FROM data WHERE age > 25")
print(results)
# [{"id": 1, "name": "Alice", "age": 30, "salary": 75000.0}]

# Delete rows
db.delete_rows_where("id = 1")
print("✓ Row deleted")
```

### Example 2: Buffered Insert (High Performance)

```python
from posixlake import DatabaseOps, Schema, Field
import json

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
    Field(name="email", data_type="String", nullable=False),
])

db = DatabaseOps.create("/path/to/db", schema)

# Insert many small batches efficiently (buffers up to 1000 rows)
print("Inserting 100 small batches using buffered insert...")
for i in range(100):
    db.insert_buffered_json(json.dumps([{
        "id": i,
        "name": f"User_{i}",
        "email": f"user{i}@example.com"
    }]))
    if (i + 1) % 20 == 0:
        print(f"  Buffered {i + 1}/100 batches...")

# Flush buffer to commit all data
print("\nFlushing write buffer...")
db.flush_write_buffer()
print("✓ All buffered data committed to Delta Lake")

# Result: ~1-2 Delta Lake transactions instead of 100!
# Performance improvement: ~10x faster for small batches
```

### Example 3: S3 / Object Storage Backend

```python
from posixlake import DatabaseOps, Schema, Field, S3Config

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
    Field(name="value", data_type="Float64", nullable=True),
])

# Create database on S3/MinIO
s3_config = S3Config(
    endpoint="http://localhost:9000",  # MinIO or AWS S3 endpoint
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    region="us-east-1"
)

db = DatabaseOps.create_with_s3("s3://bucket-name/db-path", schema, s3_config)

# Same API works with S3!
db.insert_json('[{"id": 1, "name": "Alice", "value": 123.45}]')
results = db.query_json("SELECT * FROM data WHERE value > 100")
print(results)

# All data stored in S3 with Delta Lake ACID transactions
```

### Example 4: POSIX Access via NFS Server

```python
from posixlake import DatabaseOps, Schema, Field, NfsServer
import time
import subprocess

# Create database
schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
    Field(name="age", data_type="Int32", nullable=True),
])
db = DatabaseOps.create("/path/to/db", schema)

# Insert data
db.insert_json('[{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]')

# Start NFS server on port 12049
nfs_port = 12049
nfs_server = NfsServer(db, nfs_port)
print(f"✓ NFS server started on port {nfs_port}")

# Wait for server to be ready
time.sleep(0.5)
if nfs_server.is_ready():
    print("✓ NFS server is ready!")
else:
    print("⚠ NFS server not ready, POSIX operations may fail")

# Mount filesystem (requires sudo - run this in terminal)
# sudo mount_nfs -o nolocks,vers=3,tcp,port=12049,mountport=12049 localhost:/ /mnt/posixlake

# Now use standard Unix tools to query and trigger Delta Lake operations:
# $ cat /mnt/posixlake/data/data.csv  # Queries Parquet data, converts to CSV
# id,name,age
# 1,Alice,30
# 2,Bob,25
#
# $ grep "Alice" /mnt/posixlake/data/data.csv | awk -F',' '{print $2}'  # Search and process
# Alice
#
# $ wc -l /mnt/posixlake/data/data.csv  # Count records
# 3 /mnt/posixlake/data/data.csv
#
# $ echo "3,Charlie,28" >> /mnt/posixlake/data/data.csv  # Triggers Delta Lake INSERT transaction!
#
# $ sed -i 's/Alice,30/Alice,31/' /mnt/posixlake/data/data.csv  # Triggers Delta Lake MERGE (UPDATE) transaction!
#
# $ grep -v "Bob" /mnt/posixlake/data/data.csv > /tmp/temp && cat /tmp/temp > /mnt/posixlake/data/data.csv  # Triggers MERGE (DELETE) transaction!

# Shutdown NFS server when done
# nfs_server.shutdown()
```

### Example 5: Time Travel Queries

```python
from posixlake import DatabaseOps, Schema, Field

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
])

db = DatabaseOps.create("/path/to/db", schema)

# Insert initial data
db.insert_json('[{"id": 1, "name": "Alice"}]')
version_1 = db.get_current_version()
print(f"Version 1: {version_1}")

# Insert more data
db.insert_json('[{"id": 2, "name": "Bob"}]')
version_2 = db.get_current_version()
print(f"Version 2: {version_2}")

# Query by version (historical data)
results_v1 = db.query_json_at_version("SELECT * FROM data", version_1)
print(f"Data at version {version_1}: {results_v1}")
# [{"id": 1, "name": "Alice"}]

results_v2 = db.query_json_at_version("SELECT * FROM data", version_2)
print(f"Data at version {version_2}: {results_v2}")
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

# Query by timestamp
import time
timestamp = int(time.time())
results = db.query_json_at_timestamp("SELECT * FROM data", timestamp)
print(f"Data at timestamp {timestamp}: {results}")
```

### Example 6: Import from CSV (Auto Schema Inference)

```python
from posixlake import DatabaseOps
import json

# Create database by importing CSV - schema is automatically inferred!
# Column types detected: Int64, Float64, Boolean, String
db = DatabaseOps.create_from_csv("/path/to/new_db", "/path/to/data.csv")

# Query the imported data
results = db.query_json("SELECT * FROM data LIMIT 5")
print(json.loads(results))

# Check inferred schema
schema = db.get_schema()
for field in schema.fields:
    print(f"  {field.name}: {field.data_type} (nullable={field.nullable})")
```

### Example 7: Import from Parquet

```python
from posixlake import DatabaseOps
import json

# Create database from existing Parquet file(s)
# Schema is read directly from Parquet metadata
db = DatabaseOps.create_from_parquet("/path/to/new_db", "/path/to/data.parquet")

# Supports glob patterns for multiple files
db = DatabaseOps.create_from_parquet("/path/to/db", "/data/*.parquet")

# Query the imported data
results = db.query_json("SELECT COUNT(*) as total FROM data")
print(json.loads(results))
```

### Example 8: Delta Lake Operations

```python
from posixlake import DatabaseOps, Schema, Field

db = DatabaseOps.open("/path/to/db")

# OPTIMIZE: Compact small Parquet files into larger ones
optimize_result = db.optimize()
print(f"✓ OPTIMIZE completed: {optimize_result}")

# VACUUM: Remove old files (retention period in hours)
vacuum_result = db.vacuum(retention_hours=168)  # 7 days
print(f"✓ VACUUM completed: {vacuum_result}")

# Z-ORDER: Multi-dimensional clustering for better query performance
zorder_result = db.zorder(columns=["id", "name"])
print(f"✓ Z-ORDER completed: {zorder_result}")

# Get data skipping statistics
stats = db.get_data_skipping_stats()
print(f"Data skipping stats: {stats}")
```

---

## Core Features

### Database Operations

#### Creating and Opening Databases

```python
from posixlake import DatabaseOps, Schema, Field, S3Config

# Local filesystem with explicit schema
schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
])
db = DatabaseOps.create("/path/to/db", schema)
db = DatabaseOps.open("/path/to/db")

# Import from CSV (auto schema inference)
db = DatabaseOps.create_from_csv("/path/to/db", "/path/to/data.csv")

# Import from Parquet (schema from metadata)
db = DatabaseOps.create_from_parquet("/path/to/db", "/path/to/data.parquet")
db = DatabaseOps.create_from_parquet("/path/to/db", "/data/*.parquet")  # glob pattern

# With authentication
db = DatabaseOps.create_with_auth("/path/to/db", schema, auth_enabled=True)
db = DatabaseOps.open_with_credentials("/path/to/db", credentials)

# S3 backend
s3_config = S3Config(
    endpoint="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    region="us-east-1"
)
db = DatabaseOps.create_with_s3("s3://bucket/db-path", schema, s3_config)
db = DatabaseOps.open_with_s3("s3://bucket/db-path", s3_config)
```

#### Data Insertion

```python
# Regular insert (one transaction per call)
db.insert_json('[{"id": 1, "name": "Alice"}]')

# Buffered insert (batches multiple writes)
db.insert_buffered_json('[{"id": 2, "name": "Bob"}]')
db.insert_buffered_json('[{"id": 3, "name": "Charlie"}]')
db.flush_write_buffer()  # Commit all buffered data

# MERGE (UPSERT) operation
merge_data = [
    {"id": 1, "name": "Alice Updated", "_op": "UPDATE"},
    {"id": 4, "name": "David", "_op": "INSERT"},
    {"id": 2, "_op": "DELETE"}
]
result = db.merge_json(json.dumps(merge_data), "id")
# Returns: {"rows_inserted": 1, "rows_updated": 1, "rows_deleted": 1}
```

#### SQL Queries

```python
# Basic query
results = db.query_json("SELECT * FROM data WHERE id > 0")

# Aggregations
results = db.query_json("SELECT COUNT(*) as count, AVG(age) as avg_age FROM data")

# Joins (if multiple tables)
results = db.query_json("""
    SELECT a.id, a.name, b.value 
    FROM data a 
    JOIN other_table b ON a.id = b.id
""")

# Time travel queries
results = db.query_json_at_version("SELECT * FROM data", version=5)
results = db.query_json_at_timestamp("SELECT * FROM data", timestamp=1234567890)
```

#### Row Deletion

```python
# Delete by condition
db.delete_rows_where("id = 5")
db.delete_rows_where("age < 18")
db.delete_rows_where("name LIKE '%test%'")

# Delete all rows (truncate)
db.delete_rows_where("1=1")
```

### Time Travel

posixlake supports Delta Lake's time travel feature, allowing you to query historical versions of your data:

```python
# Get current version
current_version = db.get_current_version()
print(f"Current version: {current_version}")

# Query by version
results = db.query_json_at_version("SELECT * FROM data", version=10)

# Query by timestamp
import time
timestamp = int(time.time()) - 3600  # 1 hour ago
results = db.query_json_at_timestamp("SELECT * FROM data", timestamp)

# Get version history
history = db.get_version_history()
for entry in history:
    print(f"Version {entry['version']}: {entry['timestamp']} - {entry['operation']}")
```

### Delta Lake Operations

#### OPTIMIZE (File Compaction)

```python
# Compact small Parquet files into larger ones for better query performance
result = db.optimize()
print(f"Files compacted: {result}")
```

#### VACUUM (Cleanup Old Files)

```python
# Remove old files (retention period in hours)
# Default: 168 hours (7 days)
result = db.vacuum(retention_hours=168)
print(f"Files removed: {result}")
```

#### Z-ORDER (Multi-dimensional Clustering)

```python
# Cluster data by multiple columns for better query performance
result = db.zorder(columns=["id", "name", "age"])
print(f"Z-ORDER completed: {result}")
```

#### Data Skipping Statistics

```python
# Get statistics for query optimization
stats = db.get_data_skipping_stats()
print(f"Data skipping stats: {stats}")
```

### NFS Server (POSIX Filesystem Access)

The NFS server allows you to mount your Delta Lake database as a standard POSIX filesystem. **Unix commands don't just read data - they trigger Delta Lake operations**: `cat` queries Parquet data, `grep` searches, `echo >>` triggers INSERT transactions, `sed -i` triggers MERGE (UPDATE/DELETE) transactions. All operations are ACID-compliant Delta Lake transactions.

#### Starting the NFS Server

```python
from posixlake import DatabaseOps, Schema, Field, NfsServer
import time

# Create/open database
db = DatabaseOps.open("/path/to/db")

# Start NFS server on port 12049
nfs = NfsServer(db, 12049)

# Wait for server to be ready
time.sleep(0.5)
if nfs.is_ready():
    print("✓ NFS server ready")
else:
    print("⚠ NFS server not ready")
```

#### Mounting the Filesystem

```bash
# Mount command (requires sudo)
sudo mount_nfs -o nolocks,vers=3,tcp,port=12049,mountport=12049 localhost:/ /mnt/posixlake

# Verify mount
ls -la /mnt/posixlake/
# data/
# schema.sql
# .query
```

#### Using POSIX Commands

Once mounted, your Delta Lake table is accessible like any other directory:

```bash
# 1. List directory contents
ls -la /mnt/posixlake/data/

# 2. Read all data as CSV
cat /mnt/posixlake/data/data.csv
# id,name,age
# 1,Alice,30
# 2,Bob,25

# 3. Search for specific records with grep
grep "Alice" /mnt/posixlake/data/data.csv
# 1,Alice,30

# 4. Process columns with awk
awk -F',' '{print $2, $3}' /mnt/posixlake/data/data.csv
# name age
# Alice 30
# Bob 25

# 5. Count lines/records with wc
wc -l /mnt/posixlake/data/data.csv
# 3 /mnt/posixlake/data/data.csv (includes header)

# 6. Sort data by a column
sort -t',' -k2 /mnt/posixlake/data/data.csv  # Sort by name

# 7. Append new data (triggers Delta Lake INSERT transaction!)
echo "3,Charlie,28" >> /mnt/posixlake/data/data.csv
# → Executes: Delta Lake INSERT transaction with ACID guarantees
cat /mnt/posixlake/data/data.csv
# id,name,age
# 1,Alice,30
# 2,Bob,25
# 3,Charlie,28

# 8. Edit data (triggers Delta Lake MERGE transaction - atomic INSERT/UPDATE/DELETE!)
# Example: Update Alice's age to 31
sed -i 's/Alice,30/Alice,31/' /mnt/posixlake/data/data.csv
# → Executes: Delta Lake MERGE transaction (UPDATE operation)
cat /mnt/posixlake/data/data.csv
# id,name,age
# 1,Alice,31
# 2,Bob,25
# 3,Charlie,28

# Example: Delete Bob (id=2)
grep -v "2,Bob" /mnt/posixlake/data/data.csv > /tmp/temp_data.csv
cat /tmp/temp_data.csv > /mnt/posixlake/data/data.csv
# → Executes: Delta Lake MERGE transaction (DELETE operation)
cat /mnt/posixlake/data/data.csv
# id,name,age
# 1,Alice,31
# 3,Charlie,28

# 9. Truncate table (triggers Delta Lake DELETE ALL transaction!)
rm /mnt/posixlake/data/data.csv
# → Executes: Delta Lake DELETE ALL transaction
cat /mnt/posixlake/data/data.csv
# id,name,age
```

#### Unmounting and Shutdown

```bash
# Unmount filesystem
sudo umount /mnt/posixlake
```

```python
# Shutdown NFS server
nfs.shutdown()
```

**How It Works:**
- **Read Operations** (`cat`, `grep`, `awk`, `wc`): NFS server queries Parquet files → converts to CSV on-demand → caches result
- **Append Operations** (`echo >>`): NFS server parses CSV → converts to RecordBatch → Delta Lake INSERT transaction
- **Overwrite Operations** (`sed -i`, `cat > file`): Detects INSERT/UPDATE/DELETE by comparing old vs new CSV → executes MERGE transaction (atomic INSERT/UPDATE/DELETE)
- **Delete Operations** (`rm file`): Triggers Delta Lake DELETE ALL transaction
- **No Special Drivers**: Uses OS built-in NFS client - works everywhere

### Authentication & Security

```python
from posixlake import DatabaseOps, Schema, Field, Credentials

# Create database with authentication enabled
schema = Schema(fields=[...])
db = DatabaseOps.create_with_auth("/path/to/db", schema, auth_enabled=True)

# Open with credentials
credentials = Credentials(username="admin", password="secret")
db = DatabaseOps.open_with_credentials("/path/to/db", credentials)

# User management
db.create_user("alice", "password123", role="admin")
db.delete_user("alice")

# Role-based access control
# Permissions checked automatically on all operations
```

### Backup & Restore

```python
# Full backup
backup_path = db.backup("/path/to/backup")
print(f"Backup created: {backup_path}")

# Incremental backup
backup_path = db.backup_incremental("/path/to/backup")
print(f"Incremental backup created: {backup_path}")

# Restore
db.restore("/path/to/backup")
print("✓ Database restored")
```

### Monitoring

```python
# Get real-time metrics
metrics = db.get_metrics()
print(f"Metrics: {metrics}")

# Health check
is_healthy = db.health_check()
print(f"Database healthy: {is_healthy}")

# Data skipping statistics
stats = db.get_data_skipping_stats()
print(f"Data skipping stats: {stats}")
```

---

## API Reference

### DatabaseOps

Main class for database operations.

#### Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `create(path, schema)` | Create new database | `DatabaseOps` |
| `create_from_csv(db_path, csv_path)` | Create from CSV (auto schema) | `DatabaseOps` |
| `create_from_parquet(db_path, parquet_path)` | Create from Parquet | `DatabaseOps` |
| `open(path)` | Open existing database | `DatabaseOps` |
| `create_with_auth(path, schema, auth_enabled)` | Create with authentication | `DatabaseOps` |
| `open_with_credentials(path, credentials)` | Open with credentials | `DatabaseOps` |
| `create_with_s3(s3_path, schema, s3_config)` | Create on S3 | `DatabaseOps` |
| `open_with_s3(s3_path, s3_config)` | Open from S3 | `DatabaseOps` |
| `insert_json(json_data)` | Insert data from JSON | `u64` (rows inserted) |
| `insert_buffered_json(json_data)` | Buffered insert | `u64` (rows inserted) |
| `flush_write_buffer()` | Flush buffered writes | `None` |
| `merge_json(json_data, key_column)` | MERGE (UPSERT) operation | `str` (JSON metrics) |
| `query_json(sql)` | Execute SQL query | `str` (JSON results) |
| `query_json_at_version(sql, version)` | Time travel query by version | `str` (JSON results) |
| `query_json_at_timestamp(sql, timestamp)` | Time travel query by timestamp | `str` (JSON results) |
| `delete_rows_where(condition)` | Delete rows by condition | `u64` (rows deleted) |
| `optimize()` | Compact Parquet files | `str` (result) |
| `vacuum(retention_hours)` | Remove old files | `str` (result) |
| `zorder(columns)` | Multi-dimensional clustering | `str` (result) |
| `get_current_version()` | Get current version | `i64` |
| `get_version_history()` | Get version history | `list` |
| `get_data_skipping_stats()` | Get skipping statistics | `str` (JSON) |
| `get_metrics()` | Get real-time metrics | `str` (JSON) |
| `health_check()` | Health check | `bool` |
| `backup(path)` | Full backup | `str` (backup path) |
| `backup_incremental(path)` | Incremental backup | `str` (backup path) |
| `restore(path)` | Restore from backup | `None` |

### Schema

Database schema definition.

```python
from posixlake import Schema, Field

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
    Field(name="age", data_type="Int32", nullable=True),
    Field(name="salary", data_type="Float64", nullable=True),
])
```

#### Supported Data Types

**Primitive Types:**
- `Int8`, `Int16`, `Int32`, `Int64`
- `UInt8`, `UInt16`, `UInt32`, `UInt64`
- `Float32`, `Float64`
- `String`, `LargeUtf8`, `Binary`, `LargeBinary`
- `Boolean`
- `Date32`, `Date64`
- `Timestamp`

**Complex Types:**
- `Decimal128(precision,scale)` - e.g., `Decimal128(10,2)` for currency
- `List<ElementType>` - e.g., `List<Int32>`, `List<String>`
- `Map<KeyType,ValueType>` - e.g., `Map<String,Int64>`
- `Struct<field1:Type1,field2:Type2>` - e.g., `Struct<x:Int32,y:Int32>`

### Field

Schema field definition.

```python
# Simple types
Field(name="id", data_type="Int32", nullable=False)
Field(name="price", data_type="Decimal128(10,2)", nullable=False)

# Complex types
Field(name="tags", data_type="List<String>", nullable=True)
Field(name="metadata", data_type="Map<String,String>", nullable=True)
Field(name="address", data_type="Struct<city:String,zip:Int32>", nullable=True)
```

### NfsServer

NFS server for POSIX filesystem access.

```python
nfs = NfsServer(db, port=12049)
nfs.is_ready()  # Check if server is ready
nfs.shutdown()  # Shutdown server
```

### S3Config

S3 configuration for object storage backend.

```python
s3_config = S3Config(
    endpoint="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    region="us-east-1"
)
```

### PosixLakeError

Exception class for all posixlake errors.

```python
from posixlake import PosixLakeError

try:
    db.insert_json(data)
except PosixLakeError as e:
    print(f"Error: {e}")
```

#### Error Types

- `PosixLakeError.IoError` - I/O operations
- `PosixLakeError.SerializationError` - JSON/Arrow serialization
- `PosixLakeError.DeltaLakeError` - Delta Lake operations
- `PosixLakeError.InvalidOperation` - Invalid operations
- `PosixLakeError.QueryError` - SQL query errors
- `PosixLakeError.AuthenticationError` - Authentication failures
- `PosixLakeError.PermissionDenied` - Permission errors
- `PosixLakeError.SchemaError` - Schema-related errors
- `PosixLakeError.VersionError` - Version conflicts
- `PosixLakeError.StorageError` - Storage backend errors
- `PosixLakeError.NetworkError` - Network operations
- `PosixLakeError.TimeoutError` - Operation timeouts
- `PosixLakeError.NotFound` - Resource not found
- `PosixLakeError.AlreadyExists` - Resource already exists

---

## Performance

### Buffered Inserts

**10x performance improvement** for small batch writes:

```python
# Regular insert: 100 separate Delta Lake transactions
for i in range(100):
    db.insert_json(f'[{{"id": {i}, "name": "User_{i}"}}]')
# Time: ~5-10 seconds (50-100ms per transaction)

# Buffered insert: ~1-2 batched transactions
for i in range(100):
    db.insert_buffered_json(f'[{{"id": {i}, "name": "User_{i}"}}]')
db.flush_write_buffer()
# Time: ~0.5-1 second (10x faster!)
```

**How It Works:**
- Buffers multiple small writes in memory
- Auto-flushes at 1000 rows (configurable in Rust)
- Batches all buffered data into fewer Delta Lake transactions
- Reduces transaction overhead significantly

### Efficient Operations

- Optimized data transfer between Rust and Python
- Arrow RecordBatches shared efficiently
- Minimal memory copying for large datasets

### Async Operations

- Operations run on async runtime
- Synchronous Python API for ease of use
- Optimal concurrency for I/O-bound workloads

---

## Error Handling

All Rust errors are properly mapped to Python exceptions:

```python
from posixlake import PosixLakeError

try:
    db = DatabaseOps.create("/path/to/db", schema)
    db.insert_json(data)
    results = db.query_json("SELECT * FROM data")
except PosixLakeError.IoError as e:
    print(f"I/O error: {e}")
except PosixLakeError.SerializationError as e:
    print(f"Serialization error: {e}")
except PosixLakeError.DeltaLakeError as e:
    print(f"Delta Lake error: {e}")
except PosixLakeError.InvalidOperation as e:
    print(f"Invalid operation: {e}")
except PosixLakeError as e:
    print(f"posixlake error: {e}")
```

**Error Types:**
- All errors inherit from `PosixLakeError`
- Specific error types for different failure modes
- Comprehensive error messages with context
- Stack traces preserved from Rust

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────┐
│  Python Application                     │
│  from posixlake import DatabaseOps      │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  Python API Layer                       │
│  • Type conversion                      │
│  • Error handling                       │
│  • Async runtime bridge                 │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  Rust Library (libposixlake.dylib)      │
│  • DatabaseOps                          │
│  • Delta Lake operations                │
│  • DataFusion SQL engine                │
│  • NFS server                           │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  Delta Lake Protocol                    │
│  • ACID transactions                    │
│  • Time travel                          │
│  • Parquet storage                      │
└─────────────────────────────────────────┘
```

**Key Features:**
- **Type Safety**: Automatic type conversion between Rust and Python
- **Error Handling**: Comprehensive error mapping to Python exceptions
- **Efficient Data Transfer**: Optimized data sharing via Arrow
- **Async Support**: Async runtime for optimal performance
- **Memory Safety**: Rust's memory safety guarantees

### Storage Backends

posixlake Python bindings support multiple storage backends:

- **Local Filesystem**: Standard directory paths
- **S3/MinIO**: Object storage with S3-compatible API
- **Unified API**: Same Python code works with both

---

## What Makes This Awesome

1. **Performance**: Rust-powered engine with buffered inserts (~10x faster for small batches)
2. **No Special Drivers**: NFS server uses OS built-in NFS client - zero installation
3. **Unix Commands Trigger Delta Operations**: `cat` queries data, `grep` searches, `echo >>` triggers INSERT, `sed -i` triggers MERGE (UPDATE/DELETE) - all as ACID transactions
4. **Standard Tools**: `grep`, `awk`, `sed`, `wc`, `sort` work on your data lake and trigger Delta Lake operations - no special libraries needed
5. **Smart Batching**: Auto-flushes at 1000 rows, reducing transaction overhead
6. **Delta Lake Compatible**: Tables readable by Spark, Databricks, and Athena immediately
7. **Robust**: Comprehensive error handling, async support, and testing
8. **Type Safety**: Complete type hints and comprehensive error handling
9. **Efficient**: Optimized data transfer with minimal overhead
10. **Unified Storage**: Same API works with local filesystem and S3

**Use Unix commands to query and trigger Delta Lake operations** - `cat` queries Parquet data, `grep` searches, `echo >>` triggers INSERT transactions, `sed -i` triggers MERGE (UPDATE/DELETE) transactions. No special libraries, no drivers, just mount and use standard Unix tools. Plus buffered inserts for 10x performance when loading many small batches.

---

## License

**Apache License 2.0**

Copyright 2025 posixlake Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

See [LICENSE.md](../../LICENSE.md) for the full license text.

---

## Contributing

Contributions welcome! Please follow these guidelines:

1. **Write tests first** - TDD approach for all features
2. **Run full suite** - Ensure all tests pass
3. **Update documentation** - Keep README and docs up to date
4. **Commit messages** - Use conventional commits

---

## Acknowledgments

Built with:

- [Rust](https://www.rust-lang.org/) - Systems programming language
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format
- [Apache Parquet](https://parquet.apache.org/) - Columnar file format
- [DataFusion](https://datafusion.apache.org/) - Query engine
- [Delta Lake](https://delta.io/) - Transaction log
- [ObjectStore](https://docs.rs/object_store/) - Storage abstraction

---

**Questions?** Open an [issue](https://github.com/npiesco/posixlake/issues)

**Like this project?** Star the repo and share with your data engineering team!

**PyPI Package:** https://pypi.org/project/posixlake/