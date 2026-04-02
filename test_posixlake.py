#!/usr/bin/env python3

import posixlake
import tempfile
import os
import json

print("✓ posixlake 0.3.0 imported successfully")

# Create temp directory
temp_dir = tempfile.mkdtemp()
db_path = os.path.join(temp_dir, 'test.db')
print(f"Database path: {db_path}")

# Create schema
schema = posixlake.Schema(
    fields=[
        posixlake.Field(name='id', data_type='Int32', nullable=False),
        posixlake.Field(name='name', data_type='String', nullable=False)
    ],
    primary_key=None
)

# Create database
db = posixlake.DatabaseOps.create(db_path, schema)
print("✓ Database created successfully")

# Insert data
data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
db.insert_json(data)
print("✓ Data inserted")

# Query data
result = db.query_json('SELECT * FROM data')
print(f"✓ Query result: {result}")

print("🎉 posixlake 0.3.0 is working perfectly!")