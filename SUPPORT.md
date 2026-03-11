# Support Statement

This project targets self-hosted `posixlake` CLI, library, and Python binding users.

## Release Tier

- Current tier: pre-`1.0`, production-readiness hardening in progress.
- Stability target: `v1.0.0` for the local CLI and on-disk compatibility contract documented in `docs/compatibility.md`.
- Support model: best-effort maintainer support through GitHub issues until a formal commercial support program exists.

## Supported Versions

| Version line | Status | Support window |
|---|---|---|
| Current minor (`0.1.x`) | Active | Security and reliability fixes on best effort |
| Previous minor | Maintenance only if it exists and rollback docs still reference it | Until next minor release is established |
| Older minors | Unsupported | Upgrade before requesting fixes |

## Response Targets

These are project targets, not contractual SLAs.

| Severity | Example | Initial response target | Fix / mitigation target |
|---|---|---|---|
| `P0` | Data loss, unrecoverable corruption, auth bypass | 1 business day | Immediate mitigation or rollback guidance |
| `P1` | Major reliability issue, blocked upgrade, backup/restore failure | 3 business days | Fix or documented workaround in next patch/minor |
| `P2` | Non-blocking bug, docs issue, minor regression | 5 business days | Best effort |

## Support Channels

- Security issues: follow `SECURITY.md`.
- Bugs and feature requests: GitHub issues.
- Operational incidents: include logs, `posixlake-cli health`, `posixlake-cli metrics`, platform details, and the exact version output from `posixlake-cli --version`.

## What To Include In A Support Request

- `posixlake` version and target platform
- installation path: Rust crate, CLI binary, or Python binding
- storage backend: local filesystem or S3-compatible
- auth mode enabled/disabled
- repro steps
- expected behavior
- actual behavior
- if upgrade-related: source version, target version, backup verification result, and rollback attempt status

## Support Boundaries

- Mixed-version concurrent writer deployments are not supported.
- In-place downgrade is not supported unless explicitly documented.
- Platform coverage beyond `docs/compatibility.md` is best effort only.
