# Production Readiness Checklist

This checklist is the release sign-off record for `v1.0.0`.

## Exit Rule

Ship only when all `P0` items are green and all agreed `P1` items are green or explicitly waived in release notes.

## Security

| Item | Priority | Status | Notes |
|---|---|---|---|
| `SECURITY.md` published with reporting path and patch expectations | `P0` | Green | Present |
| Threat model documented for local CLI + NFS workflows | `P0` | Green | `docs/threat-model.md` |
| Auth-enabled entry points fail closed | `P0` | Green | Covered by integration tests |
| Dependency audit gate enabled | `P0` | Green | CI `audit` job |

## Release Integrity

| Item | Priority | Status | Notes |
|---|---|---|---|
| Required CI gate documented and enforced | `P0` | Green | `.github/workflows/ci.yml` |
| Build metadata embedded in release binary | `P1` | Green | `posixlake-cli --version` |
| SBOM generated in CI | `P1` | Green | `sbom` job |
| Release checksums documented | `P1` | Green | `RELEASE.md` |
| Artifact signing in release flow | `P1` | Red | Documented goal, not yet automated |

## Operations

| Item | Priority | Status | Notes |
|---|---|---|---|
| Health and metrics commands available | `P1` | Green | CLI ops commands added |
| SLOs documented | `P1` | Green | `docs/slo.md` |
| Backup/restore, upgrade/rollback, incident runbooks published | `P1` | Green | `docs/runbooks/` |
| Failure drills automated in CI | `P1` | Green | `failure_drill_test` |

## Compatibility

| Item | Priority | Status | Notes |
|---|---|---|---|
| Compatibility matrix published | `P1` | Green | `docs/compatibility.md` |
| Upgrade safety tests present | `P1` | Green | `upgrade_compat_test` |
| Rollback validation present | `P1` | Green | backup/restore rollback coverage |
| Deprecation policy and release notes template present | `P2` | Green | docs added |

## Supportability

| Item | Priority | Status | Notes |
|---|---|---|---|
| `SUPPORT.md` published | `P2` | Green | Present |
| `RELEASE_POLICY.md` published | `P2` | Green | Present |
| Known limitations captured in release notes | `P2` | Yellow | Required at release cut |

## Open Blockers

| Item | Priority | Status | Notes |
|---|---|---|---|
| Release artifact signing automation | `P1` | Open | Needed to fully satisfy Phase 2 goal |

## Sign-Off

| Area | Owner | Status |
|---|---|---|
| Security and Compliance | TBD | Pending |
| CI/CD and Release Engineering | TBD | Pending |
| Runtime Reliability and NFS Hardening | TBD | Pending |
| Docs and Support Operations | TBD | Pending |
