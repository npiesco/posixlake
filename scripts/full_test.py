#!/usr/bin/env python3
"""
Full manual FUSE test - actually create DB and mount it
"""
import subprocess
import time
import os
import sys
from posixlake import DatabaseOps, Schema, Field, NfsServer

def run(cmd):
    """Run command and print output"""
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"stderr: {result.stderr}")
    return result.returncode

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
        ])
        db = DatabaseOps.create(f"{test_dir}/db", schema)
        db.insert_json('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]')
        print("Database created with 3 rows")
    except Exception as e:
        print(f"Failed to create database: {e}")
        sys.exit(1)
    
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
    
    # Mount filesystem
    print(f"\nMounting filesystem at {test_dir}/mount...")
    mount_cmd = f"sudo mount_nfs -o nolocks,vers=3,tcp,port={nfs_port},mountport={nfs_port} localhost:/ {test_dir}/mount"
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

