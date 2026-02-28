# Jido MemoryOS

MemoryOS is a tiered memory orchestration layer built on top of
[`jido_memory`](https://github.com/agentjido/jido_memory).

## Status
- All implementation phases in [`notes/planning`](notes/planning) are complete:
  - Phase 1 through Phase 8
  - Cross-phase acceptance scenarios `X-1` through `X-8`
- Integration coverage currently includes:
  - `phase_01_integration_test.exs` through `phase_08_integration_test.exs`
  - `cross_phase_acceptance_test.exs`

## What It Includes
- Core facade API: `Jido.MemoryOS`
  - `remember/3`, `retrieve/3`, `forget/3`, `prune/2`, `consolidate/2`, `explain_retrieval/3`
  - migration helpers: `migrate_legacy/2`, `reconcile_legacy/2`
- Control plane: `Jido.MemoryOS.MemoryManager`
  - queueing/scheduling, retries, throttling, fairness, caching, journaling/replay, idempotency
- Tiered lifecycle: short/mid/long promotion with lineage and conflict handling
- Retrieval pipeline: planner, ranker, context packing, explainability trace
- Governance and safety:
  - access policy enforcement
  - approval tokens
  - audit logging
  - retention enforcement
  - redaction/masking
- Jido integrations:
  - plugin routes (`memory_os.remember|retrieve|forget|consolidate`)
  - action modules
  - framework adapters
- Phase 8 rollout/migration modules:
  - `Jido.MemoryOS.Migration`
  - `Jido.MemoryOS.ReleaseController`

## Quick Start
```elixir
target = %{id: "agent-1", group: "group-alpha"}
opts = [server: Jido.MemoryOS.MemoryManager, actor_id: target.id, actor_group: target.group, target_group: target.group]

{:ok, _record} =
  Jido.MemoryOS.remember(target, %{class: :episodic, kind: :event, text: "user likes concise responses"}, opts)

{:ok, records} =
  Jido.MemoryOS.retrieve(target, %{tier_mode: :hybrid, text_contains: "concise", limit: 5}, opts)

{:ok, explain} =
  Jido.MemoryOS.explain_retrieval(target, %{text_contains: "concise", limit: 5}, opts)
```

## Migration and Rollout
```elixir
# Legacy backfill into MemoryOS tiers
{:ok, report} =
  Jido.MemoryOS.migrate_legacy(target,
    legacy_store: {Jido.Memory.Store.ETS, [table: :jido_memory]},
    server: Jido.MemoryOS.MemoryManager,
    actor_id: target.id
  )

# Optional rollout controller for legacy/dual_run/memory_os modes
{:ok, _pid} =
  Jido.MemoryOS.ReleaseController.start_link(
    name: :memory_os_release,
    mode: :dual_run,
    memory_server: Jido.MemoryOS.MemoryManager
  )
```

## Development
```bash
mix deps.get
mix test
```

## Planning Docs
- Index: [`notes/planning/README.md`](notes/planning/README.md)
- Phase 8 release/runbook governance: [`notes/planning/phase-08-release-governance.md`](notes/planning/phase-08-release-governance.md)
