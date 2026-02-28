# 06 - Jido Plugin, Actions, and Framework Adapters

## Plugin
`Jido.MemoryOS.Plugin` exposes action routes:
- `memory_os.remember`
- `memory_os.retrieve`
- `memory_os.forget`
- `memory_os.consolidate`

It also supports auto-capturing signals using exact/wildcard patterns and rule overrides.

```elixir
defmodule MyAgent do
  use Jido.Agent,
    name: "my_agent",
    plugins: [Jido.MemoryOS.Plugin]
end

plugin_config = %{
  manager: Jido.MemoryOS.MemoryManager,
  auto_capture: true,
  capture_signal_patterns: ["ai.llm.*", "ai.tool.*"],
  capture_rules: [
    %{pattern: "ai.llm.response", tags: ["capture:llm"], kind: :response},
    %{pattern: "ai.llm.ignore", skip: true}
  ]
}
```

## Actions
Action wrappers map directly to the facade:
- `Jido.MemoryOS.Actions.Remember`
- `Jido.MemoryOS.Actions.Retrieve`
- `Jido.MemoryOS.Actions.Forget`
- `Jido.MemoryOS.Actions.Consolidate`

Use when building declarative action pipelines in Jido.

## Framework adapters
Reference adapters for common loop styles:
- `Jido.MemoryOS.FrameworkAdapter.SingleAgent`
- `Jido.MemoryOS.FrameworkAdapter.MultiAgent`
- `Jido.MemoryOS.FrameworkAdapter.ToolHeavy`

Adapter responsibilities:
- `pre_turn/3`: retrieval + context pack preparation
- `post_turn/3`: write turn outcomes into memory
- `normalize_error/2`: consistent error mapping

Use adapters when you want framework-level integration instead of calling the facade manually in every turn.
