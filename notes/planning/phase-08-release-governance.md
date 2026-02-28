# Phase 8 Release Governance

Back to phase plan: [Phase 8](./phase-08-release-migration-evolution.md)

## 1. Versioning and Deprecation Policy

### 1.1 SemVer policy
- MemoryOS follows SemVer for public API surfaces:
  - `MAJOR`: breaking API or behavior changes in public modules (`Jido.MemoryOS`, `Jido.MemoryOS.Plugin`, `Jido.MemoryOS.Actions.*`).
  - `MINOR`: backward-compatible features (new optional params, new modules, additional telemetry fields).
  - `PATCH`: bug fixes, performance improvements, and internal refactors with no contract break.
- Runtime compatibility changes to `jido_memory` that alter required function signatures are treated as `MAJOR` unless an explicit compatibility shim is provided.

### 1.2 Deprecation windows
- Public API deprecations must include:
  - deprecation warning in docs + changelog entry
  - migration path example
  - planned removal release
- Minimum deprecation window:
  - 2 minor releases before removal for call-site API changes
  - 1 minor release for non-critical option/key renames with automatic fallback
- Emergency removals are allowed only for severe security/data-loss issues and require release notes with concrete rollback instructions.

### 1.3 Compatibility matrix
| MemoryOS | jido_memory | Status |
|---|---|---|
| `0.1.x` | `0.1.x` | Supported |
| `0.2.x` | `0.1.x` | Planned via compatibility shim (if needed) |
| `1.x` | TBD | Locked at major release planning |

## 2. Rollout and Cutover Runbook

### 2.1 Internal cohort
1. Set `ReleaseController` mode to `:dual_run`.
2. Set rollout stage to `:internal` and limit internal groups.
3. Monitor drift summary until minimum sample count and threshold pass.

### 2.2 Canary cohort
1. Keep `:dual_run`.
2. Set rollout stage to `:canary` and tune `canary_ratio`.
3. Validate SLOs and fallback counters.
4. Block cutover if drift gate fails.

### 2.3 Global cutover
1. Set rollout stage to `:global`.
2. Ensure drift gate is ready (`cutover_ready? == true`).
3. Transition mode to `:memory_os`.
4. Continue rollback trigger evaluation during the first high-traffic window.

## 3. Alerting Dashboard Specification

### 3.1 Latency/Error
- `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`
- `error_rate`
- split by mode (`legacy`, `dual_run`, `memory_os`)

### 3.2 Queue/Backpressure
- `queue_depth`
- `throttled`, `overloaded`
- dead letter growth

### 3.3 Migration/Drift
- drift average/max
- drift samples count
- fallback count
- rollback count
- migration success/failure counts

## 4. Incident Playbooks

### 4.1 SLO breach in `:memory_os`
1. Call `evaluate_rollback/2` with live snapshot.
2. If rollback triggers, mode must switch to `:legacy`.
3. Keep `fallback_enabled` true until stability returns.
4. Start reconciliation job for affected targets.

### 4.2 Drift regression during `:dual_run`
1. Freeze cutover.
2. Inspect drift samples and compare result signatures.
3. Re-run migration utility for impacted namespaces.
4. Re-open cutover gate only after drift criteria pass again.

### 4.3 Post-incident reconciliation
1. Run `reconcile_post_rollback/3` for impacted cohorts.
2. Verify `Migration.reconcile_target/2` parity reports.
3. Close incident only after parity and SLO stability are both restored.
