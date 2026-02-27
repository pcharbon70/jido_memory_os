# Phase 2 - Tiered Memory and Lifecycle

Back to index: [README](./README.md)

## Relevant Shared APIs / Interfaces
- `Jido.MemoryOS.MemoryManager`
- `Jido.MemoryOS.Query`
- `Record.metadata["mem_os"]`

## Relevant Assumptions / Defaults
- Namespace format defaults to `agent:<id>:short|mid|long`.
- Retrieval defaults to hybrid ranking (semantic optional fallback to lexical).

[ ] 2 Phase 2 - Tiered Memory Model and Lifecycle  
Description: Implement short, mid, and long memory tiers with explicit promotion, eviction, and lineage rules.

[ ] 2.1 Section - Tier Semantics and Invariants  
Description: Lock formal behavior per tier.

[ ] 2.1.1 Task - Define short/mid/long invariants  
Description: Specify what belongs in each tier and why.
[ ] 2.1.1.1 Subtask - Define short-tier constraints.  
Description: Recent working memory, strict bounded size.
[ ] 2.1.1.2 Subtask - Define mid-tier constraints.  
Description: Segment/page representation for conversation blocks.
[ ] 2.1.1.3 Subtask - Define long-tier constraints.  
Description: Durable personal/semantic memory with conflict policy.

[ ] 2.1.2 Task - Define transition rules  
Description: Prevent ambiguous lifecycle movement.
[ ] 2.1.2.1 Subtask - Define short→mid criteria.  
Description: Dialogue-chain and recency completion rules.
[ ] 2.1.2.2 Subtask - Define mid→long criteria.  
Description: Heat, recurrence, and persona relevance thresholds.
[ ] 2.1.2.3 Subtask - Define demotion/expiration criteria.  
Description: Time decay and low-value eviction logic.

[ ] 2.2 Section - Short-Term Memory  
Description: Build ingestion-first working memory.

[ ] 2.2.1 Task - Implement short-tier write path  
Description: Persist all incoming interaction events.
[ ] 2.2.1.1 Subtask - Map events to records.  
Description: Normalize class/kind/text/content/tags.
[ ] 2.2.1.2 Subtask - Attach short-tier metadata.  
Description: Add turn index, chain hints, timestamps.
[ ] 2.2.1.3 Subtask - Enforce rolling caps.  
Description: Evict oldest by policy when exceeding limits.

[ ] 2.2.2 Task - Implement short-tier maintenance  
Description: Keep short tier bounded and coherent.
[ ] 2.2.2.1 Subtask - Implement prune hooks.  
Description: Time and count-based pruning.
[ ] 2.2.2.2 Subtask - Emit transition candidates.  
Description: Queue eligible records for consolidation.
[ ] 2.2.2.3 Subtask - Ensure idempotency.  
Description: Reprocessing should not duplicate data.

[ ] 2.3 Section - Mid-Term Segment/Page Memory  
Description: Organize interaction chains into retrievable structure.

[ ] 2.3.1 Task - Implement chain segmentation  
Description: Build chain and segment units.
[ ] 2.3.1.1 Subtask - Detect chain boundaries.  
Description: Session/task/topic transition logic.
[ ] 2.3.1.2 Subtask - Chunk into segments.  
Description: Token-aware and event-aware chunking.
[ ] 2.3.1.3 Subtask - Persist segment lineage.  
Description: Link segment to source short-memory IDs.

[ ] 2.3.2 Task - Implement paging  
Description: Group segments into pages with summaries.
[ ] 2.3.2.1 Subtask - Assign page IDs.  
Description: Stable page identifiers by namespace/chain.
[ ] 2.3.2.2 Subtask - Build page-level summary metadata.  
Description: Topic/persona/time summary fields.
[ ] 2.3.2.3 Subtask - Trigger long-tier candidacy.  
Description: Mark pages for promotion evaluation.

[ ] 2.4 Section - Long-Term Personal Memory  
Description: Store durable facts/preferences and resolve conflicts safely.

[ ] 2.4.1 Task - Implement promotion policy  
Description: Promote only high-value stable information.
[ ] 2.4.1.1 Subtask - Compute promotion score.  
Description: Use heat, recurrence, and relevance.
[ ] 2.4.1.2 Subtask - Persist long-tier record.  
Description: Attach persona/topic tags and provenance.
[ ] 2.4.1.3 Subtask - Record consolidation version.  
Description: Track evolution of promoted memory.

[ ] 2.4.2 Task - Implement conflict handling  
Description: Prevent contradictory memory corruption.
[ ] 2.4.2.1 Subtask - Define fact merge strategy.  
Description: Replace/append/version by policy.
[ ] 2.4.2.2 Subtask - Keep superseded history.  
Description: Tombstones or version pointers.
[ ] 2.4.2.3 Subtask - Expose conflict reasons.  
Description: Include in explain/debug output.

[ ] 2.5 Section - Phase 2 Integration Tests  
Description: Validate tier transitions and lifecycle consistency.

[ ] 2.5.1 Task - Lifecycle scenario tests  
Description: Validate short→mid→long flow.
[ ] 2.5.1.1 Subtask - Simulate long dialog streams.  
Description: Verify threshold-driven promotions.
[ ] 2.5.1.2 Subtask - Validate lineage integrity.  
Description: Every promoted record traces back to source.
[ ] 2.5.1.3 Subtask - Validate eviction behavior.  
Description: Short-tier bounds always respected.

[ ] 2.5.2 Task - Edge-case lifecycle tests  
Description: Validate resilience under bad input/concurrency.
[ ] 2.5.2.1 Subtask - Reject invalid transitions.  
Description: Enforce invariant checks.
[ ] 2.5.2.2 Subtask - Handle duplicate events safely.  
Description: No duplicated memory entries.
[ ] 2.5.2.3 Subtask - Validate concurrent consolidation.  
Description: Consistent state under parallel jobs.
