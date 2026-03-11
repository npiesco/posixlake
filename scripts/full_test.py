#!/usr/bin/env python3
"""
Full manual FUSE test - actually create DB and mount it
"""
import subprocess
import time
import os
import sys
from posixlake import (
    DatabaseOps,
    Schema,
    Field,
    NfsServer,
    get_backup_metadata,
    get_backup_metadata_with_credentials,
    restore,
    restore_to_transaction,
    restore_to_transaction_with_credentials,
    restore_with_credentials,
    verify_backup,
    verify_backup_with_credentials,
)

def run(cmd):
    """Run command and print output"""
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"stderr: {result.stderr}")
    return result.returncode


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def parse_count_value(value):
    value = str(value).strip()
    if value.isdigit():
        return int(value)

    for line in value.splitlines():
        line = line.strip().rstrip(",")
        if line.isdigit():
            return int(line)

    raise RuntimeError(f"could not parse count value from {value!r}")

def main():
    print("\n" + "="*70)
    print("FULL POSIX COMMANDS TEST")
    print("="*70)
    
    test_dir = "/tmp/posixlake_manual_test"
    
    # Cleanup
    print("\nCleaning up...")
    run(f"rm -rf {test_dir}")
    run(f"mkdir -p {test_dir}/mount")
    
    # Create database using Python API
    print("\nCreating test database...")
    try:
        schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="name", data_type="String", nullable=False),
        ], primary_key=None)
        db = DatabaseOps.create(f"{test_dir}/db", schema)
        db.insert_json('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]')
        print("Database created with 3 rows")
    except Exception as e:
        print(f"Failed to create database: {e}")
        sys.exit(1)

    # Test newly exposed Python surface on a real database
    print("\nTesting Python API integration surface...")
    try:
        db.set_primary_key("id")
        require(db.primary_key() == "id", "primary_key() did not persist")
        print("✓ primary_key / set_primary_key")

        schema_out = db.get_schema()
        require(schema_out.primary_key == "id", "get_schema() missing primary key")
        print("✓ get_schema() primary key metadata")

        metrics = db.get_metrics()
        require(metrics.total_inserts >= 1, "metrics.total_inserts did not advance")
        print(f"✓ get_metrics() inserts={metrics.total_inserts}")

        health = db.health_check()
        require(health.status == "healthy", f"unexpected health status: {health.status}")
        print(f"✓ health_check() status={health.status}")

        version_rows = db.query_version("SELECT COUNT(*) AS cnt FROM data", 1)
        require(
            version_rows and parse_count_value(version_rows[0].values["cnt"]) == 3,
            "query_version() failed",
        )
        print("✓ query_version()")

        now_ms = int(time.time() * 1000)
        timestamp_rows = db.query_timestamp("SELECT COUNT(*) AS cnt FROM data", now_ms)
        require(
            timestamp_rows and parse_count_value(timestamp_rows[0].values["cnt"]) >= 3,
            "query_timestamp() failed",
        )
        print("✓ query_timestamp()")

        backup_path = f"{test_dir}/backup"
        incremental_path = f"{test_dir}/backup_incremental"
        restore_path = f"{test_dir}/restore"
        restore_txn_path = f"{test_dir}/restore_txn"

        db.backup(backup_path)
        backup_metadata = get_backup_metadata(backup_path)
        require(backup_metadata.total_rows >= 3, "get_backup_metadata() returned wrong row count")
        print(f"✓ get_backup_metadata() rows={backup_metadata.total_rows}")

        verification = verify_backup(backup_path)
        require(verification.schema_valid, "verify_backup() did not report valid schema")
        print(f"✓ verify_backup() files={verification.files_verified}")

        db.insert_json('[{"id": 4, "name": "Dora"}]')
        db.backup_incremental(backup_path, incremental_path)
        print("✓ backup_incremental()")

        restore(backup_path, restore_path)
        restored_db = DatabaseOps.open(restore_path)
        restored_rows = restored_db.query("SELECT COUNT(*) AS cnt FROM data")
        require(
            restored_rows and parse_count_value(restored_rows[0].values["cnt"]) == 3,
            "restore() returned wrong row count",
        )
        print("✓ restore()")

        restore_to_transaction(backup_path, restore_txn_path, backup_metadata.timestamp)
        restored_txn_db = DatabaseOps.open(restore_txn_path)
        restored_txn_rows = restored_txn_db.query("SELECT COUNT(*) AS cnt FROM data")
        require(
            restored_txn_rows and parse_count_value(restored_txn_rows[0].values["cnt"]) == 3,
            "restore_to_transaction() returned wrong row count",
        )
        print("✓ restore_to_transaction()")
    except Exception as e:
        print(f"✗ Python API integration surface failed: {e}")
        sys.exit(1)

    print("\nTesting auth-backed backup/restore helpers...")
    try:
        auth_schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="name", data_type="String", nullable=False),
        ], primary_key="id")
        auth_db_path = f"{test_dir}/auth_db"
        auth_backup_path = f"{test_dir}/auth_backup"
        auth_restore_path = f"{test_dir}/auth_restore"
        auth_restore_txn_path = f"{test_dir}/auth_restore_txn"

        auth_db = DatabaseOps.create_with_auth(auth_db_path, auth_schema, True)
        auth_db.create_user("admin", "admin_pass", ["admin"])
        admin_db = DatabaseOps.open_with_credentials(auth_db_path, "admin", "admin_pass")
        admin_db.insert_json('[{"id": 1, "name": "Secured"}]')
        admin_db.backup(auth_backup_path)

        auth_metadata = get_backup_metadata_with_credentials(auth_backup_path, "admin", "admin_pass")
        require(auth_metadata.total_rows >= 1, "get_backup_metadata_with_credentials() failed")
        print(f"✓ get_backup_metadata_with_credentials() rows={auth_metadata.total_rows}")

        auth_verification = verify_backup_with_credentials(auth_backup_path, "admin", "admin_pass")
        require(auth_verification.schema_valid, "verify_backup_with_credentials() failed")
        print(f"✓ verify_backup_with_credentials() files={auth_verification.files_verified}")

        restore_with_credentials(auth_backup_path, auth_restore_path, "admin", "admin_pass")
        auth_restored_db = DatabaseOps.open_with_credentials(auth_restore_path, "admin", "admin_pass")
        auth_restored_rows = auth_restored_db.query("SELECT COUNT(*) AS cnt FROM data")
        require(
            auth_restored_rows and parse_count_value(auth_restored_rows[0].values["cnt"]) == 1,
            "restore_with_credentials() returned wrong row count",
        )
        print("✓ restore_with_credentials()")

        restore_to_transaction_with_credentials(
            auth_backup_path,
            auth_restore_txn_path,
            auth_metadata.timestamp,
            "admin",
            "admin_pass",
        )
        auth_restored_txn_db = DatabaseOps.open_with_credentials(
            auth_restore_txn_path,
            "admin",
            "admin_pass",
        )
        auth_restored_txn_rows = auth_restored_txn_db.query("SELECT COUNT(*) AS cnt FROM data")
        require(
            auth_restored_txn_rows
            and parse_count_value(auth_restored_txn_rows[0].values["cnt"]) == 1,
            "restore_to_transaction_with_credentials() returned wrong row count",
        )
        print("✓ restore_to_transaction_with_credentials()")
    except Exception as e:
        print(f"✗ Auth-backed backup/restore helpers failed: {e}")
        sys.exit(1)

    # Test complex data types
    print("\nTesting complex data types...")
    try:
        complex_schema = Schema(fields=[
            Field(name="id", data_type="Int32", nullable=False),
            Field(name="price", data_type="Decimal128(10,2)", nullable=False),
            Field(name="tags", data_type="List<String>", nullable=True),
            Field(name="metadata", data_type="Map<String,Int64>", nullable=True),
            Field(name="address", data_type="Struct<city:String,zip:Int32>", nullable=True),
        ], primary_key=None)
        complex_db = DatabaseOps.create(f"{test_dir}/complex_db", complex_schema)
        print("✓ Complex types database created:")
        for f in complex_db.get_schema().fields:
            print(f"  - {f.name}: {f.data_type}")
    except Exception as e:
        print(f"✗ Complex types failed: {e}")
    
    # Start NFS server
    print("\nStarting NFS server...")
    nfs_port = 12099
    try:
        nfs = NfsServer(db, nfs_port)
        print(f"NFS server started on port {nfs_port}")
        time.sleep(1)
    except Exception as e:
        print(f"Failed to start NFS server: {e}")
        sys.exit(1)
    
    # Mount filesystem (OS-specific command)
    print(f"\nMounting filesystem at {test_dir}/mount...")
    mount_cmd = " ".join(nfs.get_mount_command(f"{test_dir}/mount"))
    print(f"$ {mount_cmd}")
    ret = subprocess.run(mount_cmd, shell=True, capture_output=True, text=True)
    if ret.returncode != 0:
        print(f"Mount failed: {ret.stderr}")
        sys.exit(1)
    
    time.sleep(1)
    
    # Check if mounted
    print("\nChecking if mounted...")
    ret = run(f"mount | grep {test_dir}/mount")
    
    if ret == 0:
        print("Mount appears in mount table!")
        
        # Try to access it
        print("\nTrying to list mount point...")
        run(f"ls -la {test_dir}/mount")
        
        print("\nTrying to access with Python os.listdir...")
        try:
            entries = os.listdir(f"{test_dir}/mount")
            print(f"Python can read: {entries}")
        except Exception as e:
            print(f"Python cannot read: {e}")
        
        # Comprehensive POSIX command tests
        print("\n" + "="*70)
        print("TESTING ALL POSIX COMMANDS")
        print("="*70)
        
        # Supported read operations
        print("\n[READ OPERATIONS - Should work]")
        run(f"cat {test_dir}/mount/data/data.csv 2>&1 | head -5")
        run(f"grep -c '' {test_dir}/mount/data/data.csv 2>&1")
        run(f"wc -l {test_dir}/mount/data/data.csv 2>&1")
        run(f"head -2 {test_dir}/mount/data/data.csv 2>&1")
        run(f"tail -2 {test_dir}/mount/data/data.csv 2>&1")
        run(f"awk -F',' '{{print $1}}' {test_dir}/mount/data/data.csv 2>&1 | head -3")
        run(f"sort {test_dir}/mount/data/data.csv 2>&1 | head -3")
        run(f"cut -d',' -f1 {test_dir}/mount/data/data.csv 2>&1 | head -3")
        run(f"cat {test_dir}/mount/data/data.csv 2>&1 | tr ',' '|' | head -3")
        
        # Supported write operations
        print("\n[WRITE OPERATIONS - Should work]")
        run(f"echo '999,test' >> {test_dir}/mount/data/data.csv 2>&1")
        run(f"echo 'overwrite test' > {test_dir}/mount/test_overwrite.txt 2>&1")
        run(f"cat {test_dir}/mount/test_overwrite.txt 2>&1")
        run(f"echo 'new content' | sed 's/new/modified/' > {test_dir}/mount/test_sed.txt 2>&1")
        
        # Directory operations
        print("\n[DIRECTORY OPERATIONS - Should work]")
        run(f"mkdir {test_dir}/mount/testdir 2>&1")
        run(f"ls -la {test_dir}/mount/ 2>&1")
        run(f"find {test_dir}/mount -type f 2>&1 | head -5")
        run(f"pwd 2>&1")
        run(f"cd {test_dir}/mount && pwd 2>&1")
        
        # File management
        print("\n[FILE MANAGEMENT - Should work]")
        run(f"touch {test_dir}/mount/testfile.txt 2>&1 && echo 'test' > {test_dir}/mount/testfile.txt")
        run(f"cp {test_dir}/mount/testfile.txt {test_dir}/mount/testfile2.txt 2>&1")
        run(f"mv {test_dir}/mount/testfile2.txt {test_dir}/mount/testfile_renamed.txt 2>&1")
        run(f"stat {test_dir}/mount/testfile.txt 2>&1")
        run(f"rm {test_dir}/mount/testfile_renamed.txt 2>&1")  # Delete file
        run(f"ls {test_dir}/mount/testfile_renamed.txt 2>&1")  # Should fail - file deleted
        
        # rmdir operations
        print("\n[RMDIR - Should work on empty, fail on non-empty]")
        run(f"mkdir {test_dir}/mount/empty_dir 2>&1")
        run(f"rmdir {test_dir}/mount/empty_dir 2>&1")  # Should succeed
        run(f"mkdir {test_dir}/mount/nonempty_dir 2>&1")
        run(f"touch {test_dir}/mount/nonempty_dir/file.txt 2>&1")
        run(f"rmdir {test_dir}/mount/nonempty_dir 2>&1")  # Should fail with NOTEMPTY
        
        # Metadata operations - silently ignored
        print("\n[METADATA OPERATIONS - Silently ignored (appear to work but don't change anything)]")
        run(f"touch {test_dir}/mount/testfile.txt 2>&1")  # Timestamp update ignored
        run(f"stat {test_dir}/mount/testfile.txt 2>&1 | grep Modify")  # Timestamp unchanged
        run(f"chmod 777 {test_dir}/mount/testfile.txt 2>&1")  # Permission change ignored
        run(f"stat {test_dir}/mount/testfile.txt 2>&1 | grep Access")  # Still shows default perms
        run(f"chown nobody {test_dir}/mount/testfile.txt 2>&1")  # Ownership change ignored (may fail with EPERM)
        
        # Unsupported operations - should return NFS3ERR_NOTSUPP
        print("\n[UNSUPPORTED OPERATIONS - Should fail with operation not supported]")
        run(f"ln -s {test_dir}/mount/testfile.txt {test_dir}/mount/symlink.txt 2>&1")  # Symlinks not supported
        run(f"ln {test_dir}/mount/testfile.txt {test_dir}/mount/hardlink.txt 2>&1")  # Hard links not supported
        
        print("\n" + "="*70)
        print("POSIX COMMAND TESTS COMPLETE")
        print("="*70)
        
    else:
        print("Mount NOT in mount table")
    
    # Cleanup
    print("\nCleaning up...")
    print("Unmounting...")
    run(f"sudo umount {test_dir}/mount 2>&1")
    time.sleep(1)
    
    print("Stopping NFS server...")
    try:
        del nfs
    except:
        pass
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()
