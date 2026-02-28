# 04 - Retrieval, Ranking, and Explainability

## Query model
Use `Jido.MemoryOS.Query` fields in retrieval payloads:
- `tier_mode`: `:short | :mid | :long | :hybrid | :tiered | :long_priority | :all`
- `classes`, `kinds`
- `tags_any`, `tags_all`
- `text_contains`
- `since`, `until`
- `limit`, `order`
- `persona_keys`, `topic_keys`
- `candidate_fanout`
- `semantic_provider`, `semantic_timeout_ms`
- `context_token_budget`
- `include_excluded`, `debug`

## How retrieval works
1. Planner allocates tier fanout (`Retrieval.Planner`).
2. Candidates are normalized from recalled records (`Retrieval.Candidate`).
3. Ranker computes lexical/semantic + recency/heat/persona/topic features (`Retrieval.Ranker`).
4. Context pack is token-bounded and grouped by tier/topic (`Retrieval.ContextPack`).

## Explainability API
Use `explain_retrieval/3` when you need transparency for ranking and selection.

Typical response includes:
- `query`
- `planner`
- `scored_candidates`
- `selection_rationale`
- `records`
- `context_pack`
- `semantic_provider`
- `decision_trace`
- `excluded` (if requested/debug mode)

## Pluggable semantic provider
Implement `Jido.MemoryOS.Retrieval.SemanticProvider`:

```elixir
defmodule MyApp.MemoryProvider do
  @behaviour Jido.MemoryOS.Retrieval.SemanticProvider

  @impl true
  def score(_query, candidates, _opts) do
    # Return %{candidate_key => 0.0..1.0}
    {:ok, Map.new(candidates, fn c -> {c.key, 0.5} end)}
  end
end
```

Then pass `semantic_provider: MyApp.MemoryProvider` in query or options.
