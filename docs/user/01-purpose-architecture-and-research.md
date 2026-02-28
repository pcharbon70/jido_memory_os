# 01 - Purpose, Architecture, and Research

## What this library is
`Jido.MemoryOS` is a tiered memory orchestration layer built on top of `jido_memory`.

It gives agents:
- Structured short, mid, and long memory tiers
- Deterministic lifecycle operations (ingest, consolidate, promote, prune)
- Retrieval planning, ranking, and explainable context packaging
- Governance controls (policy, approvals, masking, retention, audit)

## Why use it
Use `Jido.MemoryOS` when you need more than simple key/value or vector recall:
- Long-lived agent memory with lifecycle semantics
- Personalization with provenance and conflict handling
- Safer memory operations in multi-agent or role-based environments
- Operational controls for rollout and migration from legacy memory stores

## Research basis
This implementation aligns with the MemoryOS model in the paper:
- [Memory OS of AI Agent (arXiv:2506.06326)](https://arxiv.org/abs/2506.06326)
- Reference implementation from the paper authors: [BAI-LAB/MemoryOS](https://github.com/BAI-LAB/MemoryOS)

Related work:
- [MemOS: A Memory OS for AI System (arXiv:2507.03724)](https://arxiv.org/abs/2507.03724)

## Core components in this codebase
- Facade API: `Jido.MemoryOS`
- Control plane: `Jido.MemoryOS.MemoryManager`
- Tier lifecycle + metadata: `Jido.MemoryOS.Lifecycle`, `Jido.MemoryOS.Metadata`
- Retrieval pipeline:
  - `Jido.MemoryOS.Query`
  - `Jido.MemoryOS.Retrieval.Planner`
  - `Jido.MemoryOS.Retrieval.Ranker`
  - `Jido.MemoryOS.Retrieval.ContextPack`
- Governance and safety:
  - `Jido.MemoryOS.AccessPolicy`
  - `Jido.MemoryOS.ApprovalToken`
  - `Jido.MemoryOS.DataSafety`
  - `Jido.MemoryOS.ErrorMapping`
- Ecosystem integration:
  - `Jido.MemoryOS.Plugin`
  - `Jido.MemoryOS.Actions.*`
  - `Jido.MemoryOS.FrameworkAdapter.*`
- Migration and rollout:
  - `Jido.MemoryOS.Migration`
  - `Jido.MemoryOS.ReleaseController`
