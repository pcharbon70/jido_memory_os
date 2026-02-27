# MemoryOS Planning Index

This directory contains the phase-by-phase implementation plan for MemoryOS built on top of `jido_memory`.

## Phase Files
1. [Phase 1 - Foundation and Core Integration](./phase-01-foundation-and-core-integration.md): Establish project skeleton, config contracts, and `jido_memory` adapter boundaries.
2. [Phase 2 - Tiered Memory and Lifecycle](./phase-02-tiered-memory-and-lifecycle.md): Implement short/mid/long tier semantics, transitions, and lifecycle rules.
3. [Phase 3 - Memory Manager Control Plane](./phase-03-memory-manager-control-plane.md): Build central orchestration process for ingest/retrieve/consolidate/prune.
4. [Phase 4 - Retrieval, Ranking, Context Packaging](./phase-04-retrieval-ranking-context-packaging.md): Implement planner, ranker, and generation-ready context packs.
5. [Phase 5 - Plugin, Actions, SDK Surfaces](./phase-05-plugin-actions-sdk-surfaces.md): Expose MemoryOS through Jido plugin routes, actions, and adapter hooks.
6. [Phase 6 - Governance, Security, Data Safety](./phase-06-governance-security-data-safety.md): Add access control, auditability, retention, and privacy controls.
7. [Phase 7 - Performance, Scalability, Reliability](./phase-07-performance-scalability-reliability.md): Meet production SLOs and harden for fault tolerance.
8. [Phase 8 - Release, Migration, Evolution](./phase-08-release-migration-evolution.md): Deliver migration, rollout control, and release governance.

## Shared Conventions
- Numbering:
  - Phases: `N`
  - Sections: `N.M`
  - Tasks: `N.M.K`
  - Subtasks: `N.M.K.L`
- Tracking:
  - Every phase, section, task, and subtask uses Markdown checkboxes (`[ ]`).
- Description requirement:
  - Every phase, section, and task starts with a `Description:` line.
- Integration-test requirement:
  - Each phase ends with a final Integration Tests section.

## Shared API / Interface Contract
- `Jido.MemoryOS` facade:
  - `remember/3`, `retrieve/3`, `consolidate/2`, `forget/3`, `prune/2`, `explain_retrieval/3`
- `Jido.MemoryOS.MemoryManager`:
  - Central orchestration process for ingestion/retrieval/lifecycle.
- `Jido.MemoryOS.Config`:
  - Runtime validation and defaults for tiers, retrieval, plugin, and policy.
- `Jido.MemoryOS.Query`:
  - Tier strategy, scoring controls, persona filters, and debug behavior.
- `Jido.MemoryOS.Plugin` and actions:
  - `memory_os.remember`, `memory_os.retrieve`, `memory_os.forget`, `memory_os.consolidate`
- `Record.metadata["mem_os"]` contract:
  - `tier`, `chain_id`, `segment_id`, `page_id`, `heat`, `promotion_score`, `last_accessed_at`, `consolidation_version`, `persona_keys`

## Shared Assumptions and Defaults
- `jido_memory` is consumed as dependency core (not forked).
- Tier namespaces default to `agent:<id>:short|mid|long`.
- MVP uses ETS for all tiers; production adds durable long-term store adapter via `Jido.Memory.Store` behavior.
- API return pattern is `{:ok, value} | {:error, reason}`.
- Retrieval defaults to hybrid ranking (semantic optional with lexical fallback).

## Cross-Phase Acceptance Scenarios
- [ ] X-1 Description: Long multi-session persona recall remains consistent after multiple consolidations.
- [ ] X-2 Description: Contradictory user facts are handled deterministically with explainable lineage.
- [ ] X-3 Description: Concurrent mixed workloads satisfy latency and throughput SLOs.
- [ ] X-4 Description: Unauthorized cross-agent access is blocked and fully audited.
- [ ] X-5 Description: Sensitive memory is redacted/masked according to policy at write and retrieval time.
- [ ] X-6 Description: Crash/restart during consolidation recovers without duplicate or lost updates.
- [ ] X-7 Description: Migration from legacy `jido_memory` records preserves retrieval quality and integrity.
- [ ] X-8 Description: Canary rollout, rollback, and fallback behaviors work predictably under incident simulation.
