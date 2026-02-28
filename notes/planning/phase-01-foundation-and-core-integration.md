# Phase 1 - Foundation and Core Integration

Back to index: [README](./README.md)

## Relevant Shared APIs / Interfaces
- `Jido.MemoryOS` facade
- `Jido.MemoryOS.Config`
- `Jido.MemoryOS.MemoryManager`
- `Record.metadata["mem_os"]`

## Relevant Assumptions / Defaults
- `jido_memory` is used as dependency core (not forked).
- Namespace format defaults to `agent:<id>:short|mid|long`.
- API return shape is `{:ok, value} | {:error, reason}`.

[x] 1 Phase 1 - Foundation and `jido_memory` Core Integration  
Description: Establish project skeleton, validated config, and adapter boundaries so MemoryOS can safely build on top of `jido_memory`.

[x] 1.1 Section - Repository and Module Bootstrap  
Description: Create implementation-ready module layout and supervision boundaries.

[x] 1.1.1 Task - Define module structure  
Description: Create module namespaces and ownership boundaries.
[x] 1.1.1.1 Subtask - Create top-level modules (`Jido.MemoryOS`, `Jido.MemoryOS.MemoryManager`, `Jido.MemoryOS.Config`).  
Description: Ensure compile-ready stubs with docs/specs.
[x] 1.1.1.2 Subtask - Create action/plugin namespaces.  
Description: Prepare `actions/` and plugin modules with schema placeholders.
[x] 1.1.1.3 Subtask - Define supervisor tree shape.  
Description: Add placeholders for manager workers and maintenance workers.

[x] 1.1.2 Task - Add dependency contract with `jido_memory`  
Description: Isolate external dependency and prevent API drift risks.
[x] 1.1.2.1 Subtask - Pin dependency version and compatibility matrix.  
Description: Lock supported versions and fail fast on unsupported versions.
[x] 1.1.2.2 Subtask - Add compile-time capability checks.  
Description: Verify required runtime functions are present.
[x] 1.1.2.3 Subtask - Define compatibility policy.  
Description: Standardize handling for future `jido_memory` upgrades.

[x] 1.2 Section - Config Schema and Defaults  
Description: Define all runtime knobs and defaults as validated config.

[x] 1.2.1 Task - Implement `Jido.MemoryOS.Config` schema  
Description: Encode tier, retrieval, plugin, and safety settings.
[x] 1.2.1.1 Subtask - Add tier default policies.  
Description: Windows, caps, TTLs, promotion thresholds.
[x] 1.2.1.2 Subtask - Add retrieval default policies.  
Description: Limits, ranking weights, fallback behavior.
[x] 1.2.1.3 Subtask - Add plugin and capture defaults.  
Description: Signal patterns, auto-capture toggle, route options.

[x] 1.2.2 Task - Implement config normalization/precedence  
Description: Ensure deterministic runtime behavior.
[x] 1.2.2.1 Subtask - Merge precedence rules.  
Description: Call options > plugin state > app config.
[x] 1.2.2.2 Subtask - Normalize namespace/store values.  
Description: Resolve per-tier store and namespace values.
[x] 1.2.2.3 Subtask - Normalize and report schema errors.  
Description: Return structured error payloads with field paths.

[x] 1.3 Section - `jido_memory` Adapter Layer  
Description: Create a stable bridge that wraps `Jido.Memory.Runtime`.

[x] 1.3.1 Task - Implement runtime adapter methods  
Description: Wrap remember/get/recall/forget/prune in one module.
[x] 1.3.1.1 Subtask - Add write/read wrappers with option injection.  
Description: Auto-attach tier namespace and store config.
[x] 1.3.1.2 Subtask - Normalize upstream errors.  
Description: Map to stable MemoryOS error taxonomy.
[x] 1.3.1.3 Subtask - Add trace metadata.  
Description: Attach correlation IDs and operation tags.

[x] 1.3.2 Task - Implement MemoryOS metadata encoder/decoder  
Description: Standardize `metadata["mem_os"]`.
[x] 1.3.2.1 Subtask - Encode lifecycle fields.  
Description: Persist tier, lineage, heat, and promotion fields.
[x] 1.3.2.2 Subtask - Decode with backward-compatible defaults.  
Description: Handle missing fields safely.
[x] 1.3.2.3 Subtask - Validate metadata schema.  
Description: Reject invalid tier/lifecycle state transitions.

[x] 1.4 Section - Phase 1 Integration Tests  
Description: Validate bootstrap and compatibility end-to-end.

[x] 1.4.1 Task - Bootstrap integration tests  
Description: Ensure app startup, config loading, and adapter health.
[x] 1.4.1.1 Subtask - Start system with defaults.  
Description: Verify supervision and required processes.
[x] 1.4.1.2 Subtask - Smoke test wrapped runtime operations.  
Description: Validate remember/recall/forget via adapter.
[x] 1.4.1.3 Subtask - Assert namespace/store resolution.  
Description: Verify per-tier deterministic values.

[x] 1.4.2 Task - Contract integration tests  
Description: Validate API and error contracts are stable.
[x] 1.4.2.1 Subtask - Check metadata encoding/decoding roundtrip.  
Description: Validate lifecycle fields survive persistence.
[x] 1.4.2.2 Subtask - Check structured config errors.  
Description: Invalid config returns path-aware errors.
[x] 1.4.2.3 Subtask - Check dependency compatibility guards.  
Description: Fail fast with clear diagnostics.
