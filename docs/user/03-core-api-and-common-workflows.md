# 03 - Core API and Common Workflows

## Public facade
`Jido.MemoryOS` exposes the stable API:
- `remember/3`
- `retrieve/3`
- `forget/3`
- `prune/2`
- `consolidate/2`
- `explain_retrieval/3`
- `migrate_legacy/2`
- `reconcile_legacy/2`

## Minimal end-to-end flow

```elixir
target = %{id: "agent-1", group: "support"}
opts = [
  server: Jido.MemoryOS.MemoryManager,
  actor_id: target.id,
  actor_group: target.group,
  target_group: target.group,
  tier: :short
]

{:ok, remembered} =
  Jido.MemoryOS.remember(
    target,
    %{
      class: :episodic,
      kind: :event,
      text: "User prefers short answers",
      tags: ["persona:style", "topic:response"]
    },
    opts
  )

{:ok, results} =
  Jido.MemoryOS.retrieve(
    target,
    %{
      tier_mode: :hybrid,
      text_contains: "short answers",
      limit: 5
    },
    opts
  )

{:ok, explain} =
  Jido.MemoryOS.explain_retrieval(
    target,
    %{text_contains: "short answers", limit: 5, debug: true},
    opts
  )

{:ok, _summary} = Jido.MemoryOS.consolidate(target, opts)
{:ok, _deleted?} = Jido.MemoryOS.forget(target, remembered.id, opts)
{:ok, _pruned_count} = Jido.MemoryOS.prune(target, opts)
```

## Important behavior notes
- `remember/3` writes metadata under `metadata["mem_os"]`.
- `consolidate/2` is what drives short -> mid -> long lifecycle movement.
- `retrieve/3` returns records only.
- `explain_retrieval/3` returns records plus planner/ranker/context details.
- `forget/3` is idempotent (`{:ok, false}` if already gone).
