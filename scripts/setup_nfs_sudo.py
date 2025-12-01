#!/usr/bin/env python3
"""
Setup passwordless sudo for NFS mount/umount operations.
This is REQUIRED for FSDB NFS tests and CLI to work properly.
"""

import os
import sys
import subprocess
import platform
import tempfile

def check_sudo():
    """Check if passwordless sudo is configured by verifying sudoers file exists"""
    username = get_username()
    username_safe_filename = username.replace('.', '_')
    sudoers_file = f"/etc/sudoers.d/fsdb-nfs-{username_safe_filename}"
    
    print(f"[DEBUG] Checking for sudoers file: {sudoers_file}")
    
    # Check if file exists
    if not os.path.exists(sudoers_file):
        print(f"[DEBUG] File does not exist")
        return False
    
    print(f"[DEBUG] File exists, checking ownership and permissions")
    
    # Check file ownership and permissions
    try:
        stat_result = subprocess.run(['stat', '-f', '%u:%g %p', sudoers_file],
                                    capture_output=True,
                                    text=True)
        print(f"[DEBUG] File stat: {stat_result.stdout.strip()}")
        
        # File should be owned by root (uid 0)
        if not stat_result.stdout.startswith('0:'):
            print(f"[DEBUG] File not owned by root")
            return False
            
        print(f"[DEBUG] Sudoers file is properly configured")
        return True
    except Exception as e:
        print(f"[DEBUG] Error checking file: {e}")
        return False

def get_username():
    """Get current username"""
    return os.environ.get('USER') or os.environ.get('USERNAME')

def setup_macos():
    """Setup passwordless sudo for macOS NFS commands"""
    username = get_username()
    # Escape username for sudoers if it contains special chars
    # Also create a filename-safe version (replace . with _)
    username_safe_filename = username.replace('.', '_')
    sudoers_line = f"{username} ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount\n"
    sudoers_file = f"/etc/sudoers.d/fsdb-nfs-{username_safe_filename}"
    
    print(f"[SETUP] Setting up passwordless sudo for {username} on macOS...")
    print(f"        Commands: mount_nfs, umount")
    print(f"        Sudoers file: {sudoers_file}")
    print(f"[DEBUG] Sudoers line to write: {repr(sudoers_line)}")
    
    # Check if /etc/sudoers includes sudoers.d directory
    print(f"[DEBUG] Checking if /etc/sudoers includes sudoers.d...")
    try:
        result = subprocess.run(['grep', '-q', '@includedir /etc/sudoers.d', '/etc/sudoers'],
                              capture_output=True)
        has_at_include = (result.returncode == 0)
        
        result2 = subprocess.run(['grep', '-q', '#includedir /etc/sudoers.d', '/etc/sudoers'],
                               capture_output=True)
        has_hash_include = (result2.returncode == 0)
        
        if not has_at_include and not has_hash_include:
            print(f"[SETUP] /etc/sudoers does not include /etc/sudoers.d directory")
            print(f"[SETUP] Adding @includedir directive to /etc/sudoers...")
            
            # Read current sudoers file
            read_result = subprocess.run(['sudo', 'cat', '/etc/sudoers'],
                                        capture_output=True,
                                        text=True)
            if read_result.returncode != 0:
                print(f"[ERROR] Failed to read /etc/sudoers")
                return False
            
            # Add the includedir line at the end
            new_sudoers = read_result.stdout
            if not new_sudoers.endswith('\n'):
                new_sudoers += '\n'
            new_sudoers += '@includedir /etc/sudoers.d\n'
            
            # Write to temp file and use visudo -c to validate
            fd, temp_sudoers = tempfile.mkstemp(text=True)
            try:
                with os.fdopen(fd, 'w') as f:
                    f.write(new_sudoers)
                
                # Validate with visudo
                validate_result = subprocess.run(['sudo', 'visudo', '-c', '-f', temp_sudoers],
                                               capture_output=True)
                if validate_result.returncode != 0:
                    print(f"[ERROR] Modified sudoers file validation failed!")
                    os.unlink(temp_sudoers)
                    return False
                
                # Copy to /etc/sudoers
                print(f"[SUDO] Updating /etc/sudoers (requires password):")
                copy_result = subprocess.run(['sudo', 'cp', temp_sudoers, '/etc/sudoers'])
                os.unlink(temp_sudoers)
                
                if copy_result.returncode != 0:
                    print(f"[ERROR] Failed to update /etc/sudoers")
                    return False
                
                print(f"[SUCCESS] Added @includedir to /etc/sudoers")
            except Exception as e:
                print(f"[ERROR] Exception updating sudoers: {e}")
                if os.path.exists(temp_sudoers):
                    os.unlink(temp_sudoers)
                return False
    except Exception as e:
        print(f"[WARNING] Could not check /etc/sudoers: {e}")
    
    # Create temporary file with proper permissions
    fd, temp_file = tempfile.mkstemp(text=True)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(sudoers_line)
        
        # Set correct permissions before sudo
        os.chmod(temp_file, 0o440)
        
        # Move to sudoers.d (requires sudo) and set proper ownership
        print("\n[SUDO] This requires your sudo password ONE TIME to setup:")
        result = subprocess.run([
            'sudo', 'sh', '-c',
            f'visudo -c -f {temp_file} && mv {temp_file} {sudoers_file} && chown root:wheel {sudoers_file} && chmod 0440 {sudoers_file}'
        ])
        
        if result.returncode == 0:
            print(f"[SUCCESS] Passwordless sudo configured successfully!")
            print(f"          You can now run NFS mount/umount without password prompts")
            return True
        else:
            print(f"[ERROR] Failed to configure passwordless sudo")
            return False
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        if os.path.exists(temp_file):
            os.unlink(temp_file)
        return False

def setup_linux():
    """Setup passwordless sudo for Linux NFS commands"""
    username = get_username()
    sudoers_line = f"{username} ALL=(ALL) NOPASSWD: /bin/mount, /bin/umount\n"
    sudoers_file = f"/etc/sudoers.d/fsdb-nfs-{username}"
    
    print(f"[SETUP] Setting up passwordless sudo for {username} on Linux...")
    print(f"        Commands: mount, umount")
    print(f"        Sudoers file: {sudoers_file}")
    
    # Create temporary file with proper permissions
    fd, temp_file = tempfile.mkstemp(text=True)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(sudoers_line)
        
        # Set correct permissions before sudo
        os.chmod(temp_file, 0o440)
        
        # Move to sudoers.d (requires sudo) and set proper ownership
        print("\n[SUDO] This requires your sudo password ONE TIME to setup:")
        result = subprocess.run([
            'sudo', 'sh', '-c',
            f'visudo -c -f {temp_file} && mv {temp_file} {sudoers_file} && chown root:root {sudoers_file} && chmod 0440 {sudoers_file}'
        ])
        
        if result.returncode == 0:
            print(f"[SUCCESS] Passwordless sudo configured successfully!")
            print(f"          You can now run NFS mount/umount without password prompts")
            return True
        else:
            print(f"[ERROR] Failed to configure passwordless sudo")
            return False
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        if os.path.exists(temp_file):
            os.unlink(temp_file)
        return False

def verify_setup():
    """Verify that passwordless sudo is working"""
    print("\n[VERIFY] Verifying passwordless sudo setup...")
    
    # First check file exists
    if not check_sudo():
        print("[ERROR] Sudoers file not properly configured")
        return False
    
    # Now actually test running the command
    print("[VERIFY] Testing actual sudo mount_nfs command...")
    system = platform.system()
    
    try:
        if system == "Darwin":
            # Kill any cached sudo credentials first
            subprocess.run(['sudo', '-k'], capture_output=True)
            # Try mount_nfs with -n (non-interactive)
            result = subprocess.run(['sudo', '-n', 'mount_nfs'], 
                                  capture_output=True, 
                                  timeout=1)
        elif system == "Linux":
            subprocess.run(['sudo', '-k'], capture_output=True)
            result = subprocess.run(['sudo', '-n', 'mount', '--help'], 
                                  capture_output=True, 
                                  timeout=1)
        else:
            print("[ERROR] Unsupported OS")
            return False
        
        print(f"[DEBUG] Test command return code: {result.returncode}")
        print(f"[DEBUG] Test command stderr: {result.stderr}")
        
        # If it says "password required", it's not working
        if b'password' in result.stderr.lower():
            print("[ERROR] Sudo still requires password - sudoers file not working!")
            print("[DEBUG] Sudoers file content should be:")
            username = get_username()
            print(f"[DEBUG]   {username} ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount")
            return False
        
        print("[SUCCESS] Passwordless sudo is working!")
        return True
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        return False

def main():
    print("=" * 70)
    print("FSDB NFS Passwordless Sudo Setup")
    print("=" * 70)
    print()
    print("This script configures your system to allow FSDB to mount NFS")
    print("filesystems without password prompts. This is REQUIRED for:")
    print("  - NFS integration tests")
    print("  - `fsdb mount` CLI command")
    print()
    
    # Check if already configured
    if check_sudo():
        print("[SUCCESS] Passwordless sudo is already configured!")
        print("          No setup needed.")
        sys.exit(0)
    
    # Detect OS and setup
    system = platform.system()
    
    if system == "Darwin":
        success = setup_macos()
    elif system == "Linux":
        success = setup_linux()
    else:
        print(f"[ERROR] Unsupported OS: {system}")
        print("        Manual sudoers configuration required")
        sys.exit(1)
    
    if success:
        # Verify
        if verify_setup():
            print("\n" + "=" * 70)
            print("SETUP COMPLETE!")
            print("=" * 70)
            print("\nYou can now run:")
            print("  - cargo test --test nfs_test")
            print("  - fsdb mount /path/to/db /path/to/mount")
            print()
            sys.exit(0)
        else:
            print("\n[WARNING] Setup completed but verification failed")
            print("          You may need to open a new terminal session")
            sys.exit(1)
    else:
        print("\n[ERROR] Setup failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
