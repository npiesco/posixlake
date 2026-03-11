# Release Process

## Versioning

posixlake follows [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking API or on-disk format changes.
- **MINOR**: New features, backward-compatible.
- **PATCH**: Bug fixes, security patches.

The Rust crate version lives in `posixlake/Cargo.toml`. The Python binding version lives in `bindings/python/setup.py` and is auto-bumped by CI on each main merge.

## Release Gate

Every push to `main` must pass the full release gate before publishing:

| Check | Job | Description |
|-------|-----|-------------|
| Format | `fmt` | `cargo fmt --all -- --check` |
| Lint | `clippy` | `cargo clippy --all-targets --all-features -- -D warnings` |
| Build | `build` | Debug + release builds on Linux, macOS, Windows |
| Unit tests | `test-unit` | `cargo test --lib` on all three platforms |
| Core integration | `test-core-integration` | Auth, backup/restore, monitoring, NFS, recovery, schema migration |
| Extended integration | `test-extended-integration` | Delta native, merge, optimize, vacuum, zorder, time travel, imports, queries |
| S3 integration | `test-s3-integration` | MinIO-backed S3 tests with Docker |
| Dependency audit | `audit` | `cargo audit --deny warnings` |
| SBOM | `sbom` | CycloneDX SBOM generation |

All jobs must pass the `release-gate` aggregation job before version bump and publishing proceed.

## Build Metadata

Every release binary embeds provenance metadata, visible via `posixlake-cli --version`:

```
0.1.0
commit: abc1234
date: 2026-03-11
target: x86_64-unknown-linux-gnu
profile: release
rustc: rustc 1.82.0 (f6e511eec 2024-10-15)
```

This metadata is injected at build time by `posixlake/build.rs`.

## Artifacts

Each release produces:

| Artifact | Format | Description |
|----------|--------|-------------|
| CLI binary | Platform-native | `posixlake-cli` for Linux, macOS, Windows |
| Python wheel | `.whl` | Platform-specific wheel with native library |
| Python sdist | `.tar.gz` | Source distribution (Linux only) |
| SBOM | CycloneDX JSON | Software Bill of Materials (`bom.json`) |

## How to Release

### Automated (CI)

1. Merge to `main`.
2. CI runs full release gate.
3. On gate pass: version bump, wheel build, PyPI publish.

### Manual

1. Update version in `posixlake/Cargo.toml`.
2. Run the full test suite: `cargo test --workspace`.
3. Run lint: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`.
4. Run audit: `cargo audit --deny warnings`.
5. Build release binary: `cargo build --release -p posixlake --bin posixlake-cli`.
6. Verify build metadata: `./target/release/posixlake-cli --version`.
7. Generate SBOM: `cargo cyclonedx --format json --output-pattern bom`.
8. Tag the release: `git tag -a v{VERSION} -m "Release v{VERSION}"`.
9. Push tag: `git push origin v{VERSION}`.

## Checksums

Release binaries should be accompanied by SHA-256 checksums:

```bash
sha256sum target/release/posixlake-cli > posixlake-cli.sha256
```

## Deprecation Policy

The detailed deprecation process lives in `docs/deprecation.md`.

- Features marked deprecated remain supported for at least one minor release before removal.
- Release notes must include a `Deprecated` section using `docs/release-notes-template.md`.
- On-disk format changes require documented upgrade and rollback guidance.
- Python binding API changes follow the same deprecation window as the Rust API.
