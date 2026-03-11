# Release Policy

## Versioning

`posixlake` follows Semantic Versioning.

- `MAJOR`: breaking API, CLI, or on-disk compatibility changes
- `MINOR`: backward-compatible features and operational improvements
- `PATCH`: bug fixes, security fixes, documentation-only release mechanics

## Release Prerequisites

A release candidate is eligible only if:

- the required CI gate is green
- release artifacts build reproducibly
- compatibility, upgrade, and rollback docs are current
- no known unresolved `P0` issues remain
- agreed `P1` issues are either closed or explicitly accepted in release notes

## Required Artifacts

Every release must include:

- platform binaries defined in `RELEASE.md`
- checksums
- SBOM
- provenance/build metadata in `--version`
- release notes based on `docs/release-notes-template.md`

## Deprecation Rules

- Deprecated behavior stays available for at least one minor release before removal.
- Removals must reference the replacement path.
- On-disk changes must ship with upgrade and rollback guidance.
- `docs/deprecation.md` is the detailed policy.

## Compatibility Rules

- Optional metadata fields must deserialize safely from older layouts.
- Any supported upgrade path must have integration coverage.
- Rollback must be validated through the documented backup/restore flow for at least one supported prior layout.

## Release Cadence

- Patch releases: as needed for reliability and security.
- Minor releases: when readiness milestones are complete and upgrade notes are prepared.
- Emergency releases: allowed outside cadence for `P0`/security issues.

## Change Control

- User-visible behavior changes require release note coverage.
- New operational commands, metadata fields, or auth behavior changes require docs updates in the same change set.
- Breaking changes require explicit approval and a migration plan before merge.
