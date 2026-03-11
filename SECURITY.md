# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.0.x   | Current release    |
| < 1.0   | Pre-release, best-effort fixes |

## Reporting a Vulnerability

If you discover a security vulnerability in posixlake, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

### How to Report

1. Email **security@posixlake.dev** with a description of the vulnerability.
2. Include steps to reproduce, affected versions, and potential impact.
3. If you have a fix, attach a patch or link to a private fork.

### What to Expect

| Stage | Target |
|-------|--------|
| Acknowledgement | Within 48 hours of report |
| Triage and severity assessment | Within 5 business days |
| Fix available (Critical/High) | Within 14 days |
| Fix available (Medium/Low) | Within 30 days |
| Public disclosure | After fix is released, coordinated with reporter |

We follow [coordinated vulnerability disclosure](https://vuls.cert.org/confluence/display/Wiki/Vulnerability+Disclosure+Policy). Reporters will be credited in the release notes unless they request anonymity.

## Security Model

posixlake operates as a **local/self-hosted CLI and library**. There is no managed cloud service or centralized auth server. Security controls protect data at the database level on the local filesystem or S3 backend.

### Authentication

- Optional per-database authentication, enabled at creation time.
- Passwords hashed with **bcrypt** (cost factor 12).
- Credentials stored in `_metadata/users.json` within the database directory.
- **Fail-closed**: auth-enabled databases deny all operations without valid credentials.

### Authorization (RBAC)

Built-in roles:

| Role | Permissions |
|------|-------------|
| `admin` | Read, Write, Delete, Admin, Backup, Restore |
| `read` | Read |
| `write` | Write |

Custom roles can be composed from the six permission types: `Read`, `Write`, `Delete`, `Admin`, `Backup`, `Restore`.

### Audit Logging

All permission-checked operations are logged to `_metadata/audit.json` with:
- Timestamp, username, operation type, details, success/failure status.
- Both successful operations and denied attempts are recorded.

### Filesystem Security

- Database directories should be protected with standard POSIX permissions.
- NFS mounts inherit the security posture of the underlying database.
- S3-backed databases rely on IAM/bucket policies for transport and storage security.

## Dependency Policy

- Dependencies are pinned in `Cargo.lock`.
- `cargo audit` is run as part of the CI pipeline.
- Critical dependency vulnerabilities are treated as Critical severity for patch SLA purposes.

## Scope

The following are **in scope** for security reports:
- Authentication bypass or credential leakage.
- Authorization escalation (accessing operations beyond granted roles).
- Audit log tampering or bypass.
- Data corruption through malformed input.
- Path traversal or injection via database paths or queries.

The following are **out of scope**:
- Attacks requiring local root access on the host running posixlake.
- Denial of service via resource exhaustion on the local machine.
- Social engineering.
