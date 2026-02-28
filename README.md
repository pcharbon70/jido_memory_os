# Jido MemoryOS

MemoryOS is a tiered memory orchestration layer built on top of
[`jido_memory`](https://github.com/agentjido/jido_memory).

This repository currently includes Phase 1:
- Core facade API: `Jido.MemoryOS`
- Application supervision and `MemoryManager`
- Config schema/defaults/precedence handling
- `jido_memory` runtime adapter with normalized Jido-native errors
- Metadata encoder/decoder for `metadata["mem_os"]`
- Plugin and action scaffolding for Jido integration

## Development

```bash
mix deps.get
mix test
```

Planning documents are tracked in `notes/planning`.
