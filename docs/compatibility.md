# Compatibility Matrix

This document defines the compatibility contract for `posixlake` `0.1.x`.

## Support Levels

| Area | Status | Source of truth |
|---|---|---|
| Local CLI / Rust crate | Supported on CI-validated platforms | `.github/workflows/ci.yml` build + test matrix |
| Python bindings from source | Supported on listed Python/platform combinations | `bindings/python/setup.py`, `bindings/python/README.md` |
| PyPI wheels | Supported on published release targets | `.github/workflows/ci.yml` `publish-python` matrix |
| On-disk Delta tables | Backward-compatible across `0.0.x -> 0.1.x` legacy metadata layouts validated by integration tests | `tests/tests/upgrade_compat_test.rs` |

## OS and Architecture Matrix

| Surface | OS | Architectures | Status |
|---|---|---|---|
| Rust crate / CLI | Linux | `x86_64` | CI-validated |
| Rust crate / CLI | macOS | `x86_64`, `aarch64` | Build-validated in CI |
| Rust crate / CLI | Windows | `x86_64`, `aarch64` | Build-validated in CI |
| Python wheel | Linux | `x86_64` (`manylinux2014_x86_64`) | Release workflow target |
| Python wheel | macOS | `x86_64`, `aarch64` | Release workflow target |
| Python wheel | Windows | `x86_64`, `aarch64` | Release workflow target |

Notes:
- Linux integration coverage runs on `ubuntu-latest`.
- macOS and Windows are part of the required build and unit-test matrix, but full integration coverage is currently Linux-only.
- Python and native-library architectures must match.

## Python Version Matrix

| Installation path | Python versions | Status |
|---|---|---|
| Source install | `3.8` through `3.12` | Declared and supported |
| PyPI wheel consumption | `3.11+` recommended | Documented release expectation |
| Packaging CI | `3.11` | Explicitly validated in release workflow |

Notes:
- `bindings/python/setup.py` declares `python_requires=">=3.8"` and classifiers for `3.8` through `3.12`.
- Release automation currently builds and validates wheels using Python `3.11`.

## Upgrade and Rollback Contract

`posixlake` `0.1.x` guarantees:

- Databases created by legacy `0.0.x` metadata layouts that omit newer optional fields such as `primary_key` can be opened by current builds.
- Opening a legacy database must not rewrite or corrupt existing data files.
- Upgrade actions that add current metadata must preserve queryability of pre-upgrade data.
- Operator rollback via backup and restore must recover both data and legacy metadata layout from the backup.

Validated paths:

- Legacy metadata open: `0.0.x-style schema.json -> 0.1.x open/query`
- Metadata upgrade: `0.0.x-style schema.json -> 0.1.x set_primary_key -> reopen`
- Rollback: `0.0.x-style backup -> 0.1.x metadata/data change -> restore -> legacy state recovered`

## Non-Goals for 0.1.x

- Cross-minor downgrade in place is not supported.
- Mixed-version concurrent writers are not supported.
- Full integration coverage on macOS and Windows is not yet part of the release gate.

## Operator Guidance

- Take a verified backup before any upgrade.
- Treat rollback as restore-to-new-path, then cut traffic over after validation.
- When introducing new metadata fields, make them optional with serde defaults for at least one minor release.
