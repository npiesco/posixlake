# Deprecation Process

This document defines how `posixlake` announces, tracks, and removes deprecated behavior.

## Policy

- A feature must be announced as deprecated for at least one minor release before removal.
- Deprecations must keep existing on-disk data readable during the warning window.
- Runtime-facing changes must fail closed rather than silently changing semantics.
- Any on-disk or API removal requires an explicit migration note and rollback guidance.

## Required Artifacts

Every deprecation must include:

- A release note entry under `Deprecated`.
- A doc update describing the replacement path.
- A target removal release or review date.
- Tests proving the deprecated path still behaves as documented during the grace period.

## Communication Cadence

| Event | Channel | Timing |
|---|---|---|
| Initial deprecation announcement | Release notes + docs | First release containing the warning |
| Reminder | Release notes | Every subsequent minor release until removal |
| Removal notice | Release notes + upgrade doc | Release immediately before removal |
| Removal execution | Release notes + compatibility doc update | Removal release |

## Implementation Guidance

- Rust API: use `#[deprecated]` when practical and mention the replacement.
- CLI/API runtime behavior: emit a warning message or structured log entry before removal.
- Metadata/on-disk changes: add optional fields first, keep serde defaults, and validate upgrade plus rollback in integration tests.
- Python bindings: document deprecated calls in `bindings/python/README.md` and keep parity with the Rust deprecation window.

## Removal Checklist

1. Confirm the deprecation has been present for at least one minor release.
2. Confirm replacement docs and runbooks exist.
3. Confirm upgrade and rollback tests cover the affected path.
4. Remove the deprecated code.
5. Update `docs/compatibility.md`, release notes, and runbooks if operator action changed.
