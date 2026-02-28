defmodule Jido.MemoryOS.Phase04IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.MemoryManager

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase4-memory-manager-#{unique}"
    target = %{id: "phase4-agent-#{unique}"}

    tables = %{
      short: :"jido_memory_os_phase4_#{unique}_short",
      mid: :"jido_memory_os_phase4_#{unique}_mid",
      long: :"jido_memory_os_phase4_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: phase4_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok, manager: manager, target: target, now: System.system_time(:millisecond)}
  end

  test "persona recall retrieves durable preferences across sessions", ctx do
    opts = [server: ctx.manager]

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user prefers concise responses",
                 tags: ["persona:style", "topic:communication", "fact_key:user.style.response"],
                 chain_id: "chain:phase4:persona:1",
                 observed_at: ctx.now + 1
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user asked for weather summary",
                 tags: ["topic:weather"],
                 chain_id: "chain:phase4:persona:2",
                 observed_at: ctx.now + 2
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{
                 tier_mode: :long_priority,
                 persona_keys: ["style"],
                 text_contains: "response",
                 limit: 5
               },
               opts
             )

    assert records != []
    assert Enum.any?(records, &String.contains?(String.downcase(&1.text || ""), "concise"))
  end

  test "recency conflict scenario prefers latest fact", ctx do
    opts = [server: ctx.manager]

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user timezone is PST",
                 tags: ["topic:profile", "fact_key:user.timezone"],
                 chain_id: "chain:phase4:recency",
                 observed_at: ctx.now + 10
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user timezone is EST",
                 tags: ["topic:profile", "fact_key:user.timezone"],
                 chain_id: "chain:phase4:recency",
                 observed_at: ctx.now + 20
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{
                 tier_mode: :long,
                 tags_any: ["fact_key:user.timezone"],
                 text_contains: "timezone",
                 limit: 2
               },
               opts
             )

    assert length(records) >= 1
    assert String.contains?(String.downcase(hd(records).text || ""), "est")
  end

  test "topic-shift scenario expands from short mode to alternate tiers", ctx do
    opts = [server: ctx.manager]

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "book flights to Lisbon",
                 tags: ["topic:travel", "persona:trip", "fact_key:user.travel.plan"],
                 chain_id: "chain:phase4:topic-a",
                 observed_at: ctx.now + 30
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "compile the deployment pipeline",
                 tags: ["topic:engineering"],
                 chain_id: "chain:phase4:topic-b",
                 observed_at: ctx.now + 40
               },
               opts
             )

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{
                 tier_mode: :short,
                 topic_keys: ["travel"],
                 tags_any: ["topic:travel"],
                 text_contains: "flight",
                 limit: 3
               },
               opts
             )

    planner_stage = Enum.find(explain.decision_trace, &(&1.stage == :planner))
    fetch_stage = Enum.find(explain.decision_trace, &(&1.stage == :fetch))

    assert planner_stage.primary_tiers == [:short]
    assert planner_stage.fallback_tiers != []
    assert fetch_stage.fallback_candidates >= 1
    assert explain.result_count >= 1
  end

  test "context pack enforces token budget and includes provenance", ctx do
    opts = [server: ctx.manager]

    for idx <- 1..5 do
      assert {:ok, _} =
               Jido.MemoryOS.remember(
                 ctx.target,
                 %{
                   class: :episodic,
                   kind: :event,
                   text:
                     "long context item #{idx}: " <>
                       String.duplicate("detailed memory content ", 20),
                   tags: ["topic:context", "fact_key:user.context.#{idx}"],
                   chain_id: "chain:phase4:pack",
                   observed_at: ctx.now + 50 + idx
                 },
                 opts
               )
    end

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{
                 tier_mode: :hybrid,
                 text_contains: "context item",
                 limit: 5,
                 context_token_budget: 80
               },
               opts
             )

    pack = explain.context_pack

    assert pack.tokens_used <= pack.token_budget
    assert pack.token_budget == 80
    assert is_boolean(pack.truncated)
    assert pack.groups != []

    entries =
      pack.groups
      |> Enum.flat_map(& &1.entries)

    assert entries != []

    assert Enum.all?(entries, fn entry ->
             is_map(entry.provenance) and is_binary(entry.provenance.source_id) and
               is_binary(entry.provenance.source_namespace)
           end)
  end

  test "explain payload includes full decision trace with semantic degradation fallback", ctx do
    opts = [server: ctx.manager]

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user likes direct, practical guidance",
                 tags: ["persona:style", "topic:assistant", "fact_key:user.style.guidance"],
                 chain_id: "chain:phase4:explain",
                 observed_at: ctx.now + 80
               },
               opts
             )

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{
                 tier_mode: :hybrid,
                 text_contains: "guidance",
                 semantic_provider: __MODULE__.SlowSemanticProvider,
                 semantic_timeout_ms: 5,
                 limit: 3
               },
               opts
             )

    assert is_map(explain.semantic_provider)
    assert explain.semantic_provider.degraded? == true
    assert explain.semantic_provider.provider == Jido.MemoryOS.Retrieval.SemanticProvider.Lexical

    assert Enum.any?(explain.decision_trace, &(&1.stage == :planner))
    assert Enum.any?(explain.decision_trace, &(&1.stage == :fetch))
    assert Enum.any?(explain.decision_trace, &(&1.stage == :rank))
    assert Enum.any?(explain.decision_trace, &(&1.stage == :context_pack))

    assert is_map(explain.selection_rationale)
    assert explain.selection_rationale.tie_breaker == "final_score desc, observed_at desc, id asc"
    assert is_map(explain.selection_rationale.weights)

    assert explain.scored_candidates != []
    assert is_list(explain.excluded)
  end

  defp phase4_app_config(tables) do
    %{
      tiers: %{
        short: %{
          store: {Jido.Memory.Store.ETS, [table: tables.short]},
          max_records: 200,
          ttl_ms: 3_600_000,
          promotion_threshold: 0.2
        },
        mid: %{
          store: {Jido.Memory.Store.ETS, [table: tables.mid]},
          max_records: 2_000,
          ttl_ms: 86_400_000,
          promotion_threshold: 0.5
        },
        long: %{
          store: {Jido.Memory.Store.ETS, [table: tables.long]},
          max_records: 20_000,
          ttl_ms: 2_592_000_000,
          promotion_threshold: 0.5
        }
      },
      lifecycle: %{
        segment_max_events: 2,
        segment_max_tokens: 256,
        page_max_segments: 1,
        conflict_strategy: :version,
        promotion_min_score: 0.5
      },
      manager: %{
        queue_max_depth: 128,
        queue_per_agent: 16,
        request_timeout_ms: 2_000,
        retry_attempts: 2,
        retry_backoff_ms: 10,
        retry_jitter_ms: 5,
        dead_letter_limit: 100,
        consolidation_debounce_ms: 20,
        auto_consolidate: false
      }
    }
  end

  defp cleanup_tables(tables) do
    Enum.each(Map.values(tables), &cleanup_store_tables/1)
  end

  defp cleanup_store_tables(base) do
    Enum.each(
      [base, :"#{base}_records", :"#{base}_ns_time", :"#{base}_ns_class_time", :"#{base}_ns_tag"],
      fn table ->
        if :ets.whereis(table) != :undefined do
          :ets.delete(table)
        end
      end
    )
  end

  defmodule SlowSemanticProvider do
    @moduledoc false
    @behaviour Jido.MemoryOS.Retrieval.SemanticProvider

    @impl true
    def score(_query, _candidates, _opts) do
      Process.sleep(25)
      {:ok, %{}}
    end
  end
end
