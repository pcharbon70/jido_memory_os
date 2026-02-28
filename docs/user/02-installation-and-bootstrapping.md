# 02 - Installation and Bootstrapping

## Dependencies
This project depends on:
- `jido`
- `jido_action`
- `jido_memory`

If you are embedding this library in another Mix project, add it as a dependency (path or git), then run:

```bash
mix deps.get
```

## Start the application
The MemoryOS OTP application starts:
- `Jido.MemoryOS.MemoryManager`
- worker processes for retrieval, consolidation, and pruning

In most apps, starting your OTP application is enough.

## Configure MemoryOS
Set config under `:jido_memory_os, Jido.MemoryOS.Config`.

```elixir
# config/runtime.exs or config/dev.exs
config :jido_memory_os, Jido.MemoryOS.Config,
  namespace_template: "agent:%{agent_id}:%{tier}",
  tiers: %{
    short: %{store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_short]}},
    mid: %{store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_mid]}},
    long: %{store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_long]}}
  }
```

## Start an isolated manager (tests or local sandbox)

```elixir
{:ok, _pid} =
  Jido.MemoryOS.MemoryManager.start_link(
    name: :memory_os_local,
    app_config: %{
      tiers: %{
        short: %{store: {Jido.Memory.Store.ETS, [table: :memos_short]}},
        mid: %{store: {Jido.Memory.Store.ETS, [table: :memos_mid]}},
        long: %{store: {Jido.Memory.Store.ETS, [table: :memos_long]}}
      }
    }
  )
```

Use `server: :memory_os_local` in calls when not using the default manager name.

## Standard runtime options
Common options you will pass to API calls:
- `server`: manager process name/pid
- `tier`: `:short | :mid | :long`
- `actor_id`, `actor_group`, `actor_role`: for policy evaluation
- `target_group`: for cross-agent access rules
- `correlation_id`: trace id propagated into metadata
- `call_timeout`, `timeout_ms`
- `store`, `store_opts`, `namespace` (advanced overrides)
