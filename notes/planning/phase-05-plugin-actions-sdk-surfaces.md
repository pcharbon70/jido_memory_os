# Phase 5 - Plugin, Actions, SDK Surfaces

Back to index: [README](./README.md)

## Relevant Shared APIs / Interfaces
- `Jido.MemoryOS.Plugin`
- Actions: `memory_os.remember`, `memory_os.retrieve`, `memory_os.forget`, `memory_os.consolidate`
- `Jido.MemoryOS` facade compatibility helpers

## Relevant Assumptions / Defaults
- API return shape remains `{:ok, value} | {:error, reason}`.
- Existing `jido_memory` users can migrate incrementally through helper mappers.

[ ] 5 Phase 5 - Jido Plugin, Actions, and Adapter Interfaces  
Description: Expose MemoryOS cleanly to Jido agents and framework-style loops.

[ ] 5.1 Section - Plugin Lifecycle  
Description: Implement robust mount/handle/restore behavior.

[ ] 5.1.1 Task - Implement `Jido.MemoryOS.Plugin` mount/state  
Description: Validate and store plugin runtime config.
[ ] 5.1.1.1 Subtask - Define plugin state schema.  
Description: Namespaces, store options, capture policies.
[ ] 5.1.1.2 Subtask - Initialize manager bindings.  
Description: Resolve manager route and runtime context.
[ ] 5.1.1.3 Subtask - Validate restore/checkpoint semantics.  
Description: Ensure safe plugin state persistence.

[ ] 5.1.2 Task - Implement signal handling  
Description: Route capture events into MemoryOS ingestion.
[ ] 5.1.2.1 Subtask - Match signal patterns.  
Description: Support exact and wildcard matching.
[ ] 5.1.2.2 Subtask - Map signals to memory payloads.  
Description: Normalize source/type/metadata fields.
[ ] 5.1.2.3 Subtask - Apply capture rules.  
Description: Support skip/override/tag injection.

[ ] 5.2 Section - Actions and API Surfaces  
Description: Provide action-level access to core operations.

[ ] 5.2.1 Task - Implement MemoryOS actions  
Description: Build run handlers for remember/retrieve/forget/consolidate.
[ ] 5.2.1.1 Subtask - Define action schemas.  
Description: Required/optional fields and defaults.
[ ] 5.2.1.2 Subtask - Implement action run pipelines.  
Description: Validate, call manager, return standardized outputs.
[ ] 5.2.1.3 Subtask - Add configurable output keys.  
Description: Workflow-friendly result key overrides.

[ ] 5.2.2 Task - Add compatibility helpers  
Description: Smooth migration from direct `jido_memory` usage.
[ ] 5.2.2.1 Subtask - Add input mapper.  
Description: Translate legacy payloads to MemoryOS inputs.
[ ] 5.2.2.2 Subtask - Add query mapper.  
Description: Translate legacy recall filters.
[ ] 5.2.2.3 Subtask - Add result mapper.  
Description: Preserve expected downstream key names.

[ ] 5.3 Section - Framework Adapter Contracts  
Description: Define hook interfaces for external agent loops.

[ ] 5.3.1 Task - Implement hook interface  
Description: Pre-turn retrieval and post-turn remember hooks.
[ ] 5.3.1.1 Subtask - Pre-turn hook contract.  
Description: Return context pack + retrieval explanation.
[ ] 5.3.1.2 Subtask - Post-turn hook contract.  
Description: Ingest response/tool traces.
[ ] 5.3.1.3 Subtask - Error contract.  
Description: Normalize external framework errors.

[ ] 5.3.2 Task - Provide reference adapters  
Description: Ship baseline adapter implementations.
[ ] 5.3.2.1 Subtask - Single-agent adapter.  
Description: Standard request-response loop.
[ ] 5.3.2.2 Subtask - Multi-agent adapter.  
Description: Shared memory orchestration.
[ ] 5.3.2.3 Subtask - Tool-heavy adapter.  
Description: Strong support for tool event memory capture.

[ ] 5.4 Section - Phase 5 Integration Tests  
Description: Validate plugin/actions/adapters end-to-end.

[ ] 5.4.1 Task - Plugin and action integration tests  
Description: Verify route wiring and state behavior.
[ ] 5.4.1.1 Subtask - Route registration tests.  
Description: All actions exposed and callable.
[ ] 5.4.1.2 Subtask - Signal capture tests.  
Description: Matching events produce memory writes.
[ ] 5.4.1.3 Subtask - Restore/checkpoint tests.  
Description: State survives roundtrip correctly.

[ ] 5.4.2 Task - Adapter integration tests  
Description: Verify hook contracts under real flows.
[ ] 5.4.2.1 Subtask - Single-agent roundtrip test.  
Description: Retrieve before turn, remember after turn.
[ ] 5.4.2.2 Subtask - Multi-agent contention test.  
Description: Isolation/fairness behavior.
[ ] 5.4.2.3 Subtask - Tool-heavy flow test.  
Description: Capture and recall tool outcomes reliably.
