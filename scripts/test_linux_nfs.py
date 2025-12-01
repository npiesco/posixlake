#!/usr/bin/env python3
"""
Test NFS mounting in Linux container (Docker/Podman)
Installs NFS client tools and runs full NFS test suite
"""

import subprocess
import sys
import os


def run_command(cmd, description):
    """Run a command and print its output"""
    print(f"\n{'='*60}")
    print(f"=== {description}")
    print(f"{'='*60}")
    
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=False,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] {description} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    
    print(f"[SUCCESS] {description} completed")
    return result


def check_linux():
    """Verify we're running on Linux"""
    if not sys.platform.startswith('linux'):
        print(f"[ERROR] This script must run on Linux, detected: {sys.platform}")
        sys.exit(1)
    print("[INFO] Running on Linux ✓")


def install_nfs_client():
    """Install NFS client tools (nfs-common), ca-certificates, sudo, and util-linux (for unshare)"""
    run_command(
        "apt-get update -qq && apt-get install -y -qq nfs-common ca-certificates sudo util-linux",
        "Installing NFS Client Tools (nfs-common + ca-certificates + sudo + util-linux)"
    )


def setup_passwordless_sudo():
    """Setup passwordless sudo for mount/umount in container"""
    print(f"\n{'='*60}")
    print(f"=== Configuring Passwordless Sudo for NFS (Container)")
    print(f"={'='*60}")
    
    username = os.environ.get('USER', 'root')
    sudoers_file = f"/etc/sudoers.d/posixlake-nfs-{username.replace('.', '_')}"
    
    # For Linux container, just configure directly (we're root)
    sudoers_content = f"{username} ALL=(ALL) NOPASSWD: /bin/mount, /bin/umount\n"
    
    # Write sudoers file
    with open(sudoers_file, 'w') as f:
        f.write(sudoers_content)
    
    # Set proper permissions
    os.chmod(sudoers_file, 0o440)
    
    print(f"[SUCCESS] Passwordless sudo configured for {username}")
    print(f"          File: {sudoers_file}")


def build_posixlake():
    """Build posixlake in release mode"""
    # Configure git and cargo for container environment
    run_command(
        "git config --global http.sslVerify false",
        "Configuring Git SSL (container environment)"
    )
    
    # Use git CLI instead of libgit2 (respects git config)
    os.environ['CARGO_NET_GIT_FETCH_WITH_CLI'] = 'true'
    
    run_command(
        "cargo build --release --workspace",
        "Building posixlake (release mode with git CLI)"
    )


def run_nfs_tests():
    """Run NFS integration tests"""
    os.environ['RUST_LOG'] = 'info'
    
    run_command(
        "cargo test --test nfs_test -- --nocapture --test-threads=1",
        "Running NFS Integration Tests"
    )


def main():
    """Main test execution"""
    print("""
╔═══════════════════════════════════════════════════════════╗
║  posixlake Linux NFS Testing (Docker/Podman Container)         ║
╚═══════════════════════════════════════════════════════════╝
""")
    
    try:
        check_linux()
        install_nfs_client()
        setup_passwordless_sudo()
        build_posixlake()
        run_nfs_tests()
        
        print(f"\n{'='*60}")
        print("✓ ALL LINUX NFS TESTS PASSED")
        print(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Test execution cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

