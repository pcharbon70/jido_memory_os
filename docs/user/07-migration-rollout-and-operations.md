# 07 - Migration, Rollout, and Operations

## Legacy migration
Use `Jido.MemoryOS.migrate_legacy/2` to backfill `jido_memory` data into MemoryOS tiers.

```elixir
target = %{id: "agent-1", group: "support"}

{:ok, report} =
  Jido.MemoryOS.migrate_legacy(target,
    legacy_store: {Jido.Memory.Store.ETS, [table: :jido_memory]},
    server: Jido.MemoryOS.MemoryManager,
    actor_id: target.id,
    dry_run: false
  )

{:ok, reconciliation} =
  Jido.MemoryOS.reconcile_legacy(target,
    legacy_store: {Jido.Memory.Store.ETS, [table: :jido_memory]},
    server: Jido.MemoryOS.MemoryManager
  )
```

## Rollout control
`Jido.MemoryOS.ReleaseController` supports:
- modes: `:legacy | :dual_run | :memory_os`
- drift sampling and cutover gating
- fallback on runtime failures
- rollback evaluation from SLO snapshots
- post-rollback reconciliation

```elixir
{:ok, _pid} =
  Jido.MemoryOS.ReleaseController.start_link(
    name: :memory_os_release,
    mode: :dual_run,
    memory_server: Jido.MemoryOS.MemoryManager,
    fallback_enabled: true
  )

{:ok, status} = Jido.MemoryOS.ReleaseController.status(:memory_os_release)
{:ok, ready?} = Jido.MemoryOS.ReleaseController.cutover_ready?(:memory_os_release)
```

## Operational APIs
From `Jido.MemoryOS.MemoryManager`:
- `metrics/1`
- `audit_events/2`
- `journal_events/2`
- `dead_letters/1`
- `last_conflicts/1`
- `cancel_pending/2`
- `current_config/1`

## Benchmark helpers
`Jido.MemoryOS.Performance` includes:
- `benchmark_ingestion/4`
- `benchmark_retrieval/4`
- `benchmark_mixed/4`
- `default_slos/0`

Use these for repeatable throughput/latency checks before cutover.

## Quick production checklist
1. Validate config with realistic tier sizes and TTLs.
2. Enable governance policy and masking for your roles.
3. Run migration in `dry_run: true` first.
4. Start with `:dual_run` rollout and monitor drift.
5. Cut over only when drift/SLO gates pass.
6. Keep fallback enabled until post-cutover stability is proven.
