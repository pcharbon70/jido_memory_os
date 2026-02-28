defmodule Jido.MemoryOS.Phase05IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.Actions.{Consolidate, Forget, Remember, Retrieve}
  alias Jido.MemoryOS.FrameworkAdapter.{MultiAgent, SingleAgent, ToolHeavy}
  alias Jido.MemoryOS.MemoryManager

  defmodule PluginAgent do
    use Jido.Agent,
      name: "phase5_plugin_agent",
      plugins: [Jido.MemoryOS.Plugin]
  end

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase5-memory-manager-#{unique}"
    target = %{id: "phase5-agent-#{unique}"}

    tables = %{
      short: :"jido_memory_os_phase5_#{unique}_short",
      mid: :"jido_memory_os_phase5_#{unique}_mid",
      long: :"jido_memory_os_phase5_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: phase5_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok, manager: manager, target: target, tables: tables, now: System.system_time(:millisecond)}
  end

  test "plugin registers all routes and routes actions are callable", ctx do
    assert {"memory_os.remember", Remember, -10} in PluginAgent.plugin_routes()
    assert {"memory_os.retrieve", Retrieve, -10} in PluginAgent.plugin_routes()
    assert {"memory_os.forget", Forget, -10} in PluginAgent.plugin_routes()
    assert {"memory_os.consolidate", Consolidate, -10} in PluginAgent.plugin_routes()
    assert {:ok, _jido} = Jido.start(name: Jido)

    server =
      start_supervised!(
        {Jido.AgentServer,
         agent: PluginAgent, id: "phase5-plugin-agent-#{System.unique_integer([:positive])}"},
        id: {:phase5_agent_server, System.unique_integer([:positive])}
      )

    remember_signal =
      signal!(
        "memory_os.remember",
        memory_runtime_params(ctx, %{
          class: :episodic,
          kind: :event,
          text: "phase5 plugin route remember",
          chain_id: "chain:phase5:plugin",
          memory_result_key: :last_memory_id
        })
      )

    assert {:ok, remembered_agent} = Jido.AgentServer.call(server, remember_signal)
    memory_id = remembered_agent.state.last_memory_id
    assert is_binary(memory_id)

    retrieve_signal =
      signal!(
        "memory_os.retrieve",
        memory_runtime_params(ctx, %{
          text_contains: "plugin route remember",
          tier_mode: :short,
          limit: 5,
          memory_result_key: :records
        })
      )

    assert {:ok, retrieved_agent} = Jido.AgentServer.call(server, retrieve_signal)
    assert retrieved_agent.state.records != []

    forget_signal =
      signal!(
        "memory_os.forget",
        memory_runtime_params(ctx, %{
          id: memory_id,
          memory_result_key: :deleted
        })
      )

    assert {:ok, forgotten_agent} = Jido.AgentServer.call(server, forget_signal)
    assert forgotten_agent.state.deleted == true

    consolidate_signal =
      signal!(
        "memory_os.consolidate",
        memory_runtime_params(ctx, %{
          memory_result_key: :summary
        })
      )

    assert {:ok, consolidated_agent} = Jido.AgentServer.call(server, consolidate_signal)
    assert is_map(consolidated_agent.state.summary)
  end

  test "plugin handle_signal captures events with wildcard rules and skip overrides", ctx do
    plugin_config = %{
      manager: ctx.manager,
      tier: :short,
      store: {Jido.Memory.Store.ETS, [table: ctx.tables.short]},
      auto_capture: true,
      capture_signal_patterns: ["ai.llm.*", "ai.tool.*"],
      capture_default_tags: ["capture:default"],
      capture_rules: [
        %{pattern: "ai.llm.response", tags: ["capture:llm"], kind: :response},
        %{pattern: "ai.tool.result", tags: ["capture:tool"], tier: :short},
        %{pattern: "ai.llm.ignore", skip: true}
      ]
    }

    assert {:ok, plugin_state} = Jido.MemoryOS.Plugin.mount(%{}, plugin_config)
    context = plugin_context(ctx.target, plugin_state)

    signal = signal!("ai.llm.response", %{response: "captured plugin response"})
    assert {:ok, :continue} = Jido.MemoryOS.Plugin.handle_signal(signal, context)

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "captured plugin response", tier_mode: :short, limit: 10},
               memory_runtime_opts(ctx)
             )

    assert length(records) >= 1
    assert Enum.any?(records, fn record -> "capture:llm" in (record.tags || []) end)

    ignored = signal!("ai.llm.ignore", %{response: "should not capture"})
    assert {:ok, :continue} = Jido.MemoryOS.Plugin.handle_signal(ignored, context)

    assert {:ok, ignored_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "should not capture", tier_mode: :short, limit: 10},
               memory_runtime_opts(ctx)
             )

    assert ignored_records == []
  end

  test "plugin checkpoint and restore keep normalized state semantics", _ctx do
    config = %{
      manager: :phase5_memory_manager,
      manager_opts: [call_timeout: 5_000],
      tier: :short,
      capture_signal_patterns: ["ai.*"],
      capture_rules: [%{pattern: "ai.llm.response", tags: ["phase5"]}]
    }

    assert {:ok, plugin_state} = Jido.MemoryOS.Plugin.mount(%{}, config)
    assert :keep = Jido.MemoryOS.Plugin.on_checkpoint(plugin_state, %{})

    assert {:ok, restored} = Jido.MemoryOS.Plugin.on_restore(plugin_state, %{})
    assert restored.bindings.server == :phase5_memory_manager
    assert restored.capture.patterns == ["ai.*"]
    assert restored.capture.rules != []

    assert :drop = Jido.MemoryOS.Plugin.on_checkpoint(:invalid, %{})

    assert {:error, :invalid_plugin_state_pointer} =
             Jido.MemoryOS.Plugin.on_restore(:invalid, %{})
  end

  test "compatibility mappers translate legacy input/query/result shapes", _ctx do
    input = %{
      "tier" => "short",
      "namespace" => "legacy:ns",
      "text" => "legacy payload",
      "tag" => "legacy",
      "store_opts" => [table: :legacy]
    }

    mapped_input = Jido.MemoryOS.Compatibility.map_legacy_input(input)
    assert mapped_input.attrs["text"] == "legacy payload"
    assert Keyword.get(mapped_input.opts, :namespace) == "legacy:ns"

    query =
      Jido.MemoryOS.Compatibility.map_legacy_query(%{
        "query" => "legacy payload",
        "tag" => "legacy",
        "max_results" => "2",
        "sort" => "desc"
      })

    assert query[:text_contains] == "legacy payload"
    assert query[:tags_any] == ["legacy"]
    assert query[:limit] == 2
    assert query[:order] == :desc

    mapped_result =
      Jido.MemoryOS.Compatibility.map_legacy_result(%{
        memory_results: [%{id: "1"}],
        last_memory_id: "id-1",
        memory_consolidation: %{status: :ok}
      })

    assert mapped_result[:memories] == [%{id: "1"}]
    assert mapped_result[:memory_id] == "id-1"
    assert mapped_result[:consolidation] == %{status: :ok}
  end

  test "single-agent adapter retrieves before turn and remembers after turn", ctx do
    opts = memory_runtime_opts(ctx)

    assert {:ok, post_turn} =
             SingleAgent.post_turn(
               ctx.target,
               %{
                 response_text: "user prefers concise responses",
                 tags: ["persona:style"],
                 chain_id: "chain:phase5:single"
               },
               opts
             )

    assert is_binary(post_turn.memory_id)

    assert {:ok, pre_turn} =
             SingleAgent.pre_turn(
               ctx.target,
               %{memory_query: %{text: "concise responses", tier_mode: :short, limit: 3}},
               opts
             )

    assert pre_turn.retrieval.result_count >= 1
    assert is_map(pre_turn.context_pack)
  end

  test "multi-agent adapter enforces fair per-target retrieval and shared writes", ctx do
    opts = memory_runtime_opts(ctx)

    target_a = %{id: "#{ctx.target.id}-a"}
    target_b = %{id: "#{ctx.target.id}-b"}
    target_shared = %{id: "#{ctx.target.id}-shared"}

    for target <- [target_a, target_b, target_shared], idx <- 1..2 do
      assert {:ok, _} =
               Jido.MemoryOS.remember(
                 target,
                 %{
                   class: :episodic,
                   kind: :event,
                   text: "shared project context #{idx} for #{target.id}",
                   chain_id: "chain:phase5:multi:#{target.id}"
                 },
                 opts
               )
    end

    assert {:ok, pre_turn} =
             MultiAgent.pre_turn(
               target_a,
               %{
                 memory_query: %{text: "shared project context", tier_mode: :short, limit: 3},
                 shared_targets: [target_b, target_shared]
               },
               opts
             )

    assert pre_turn.retrieval.per_target_limit == 1

    assert Map.keys(pre_turn.records_by_agent) |> Enum.sort() ==
             Enum.sort([target_a.id, target_b.id, target_shared.id])

    assert Enum.all?(pre_turn.records_by_agent, fn {_agent_id, records} ->
             length(records) <= 1
           end)

    assert {:ok, post_turn} =
             MultiAgent.post_turn(
               target_a,
               %{
                 response: "phase5 shared decision recorded",
                 shared_targets: [target_b, target_shared],
                 chain_id: "chain:phase5:multi:shared"
               },
               Keyword.put(opts, :broadcast_shared, true)
             )

    assert length(post_turn.written) == 3

    for target <- [target_a, target_b, target_shared] do
      assert {:ok, records} =
               Jido.MemoryOS.retrieve(
                 target,
                 %{text_contains: "shared decision recorded", tier_mode: :short, limit: 5},
                 opts
               )

      assert records != []
    end
  end

  test "tool-heavy adapter captures tool traces and recalls them with tool tags", ctx do
    opts = memory_runtime_opts(ctx)

    assert {:ok, post_turn} =
             ToolHeavy.post_turn(
               ctx.target,
               %{
                 response_text: "completed weather and calendar tools",
                 chain_id: "chain:phase5:tools",
                 tool_events: [
                   %{tool_name: "weather_lookup", status: :ok, result: "72F and sunny"},
                   %{tool_name: "calendar_create", status: :ok, result: %{event_id: "evt-1"}}
                 ]
               },
               opts
             )

    assert is_binary(post_turn.assistant_memory_id)
    assert length(post_turn.tool_memory_ids) == 2

    assert {:ok, pre_turn} =
             ToolHeavy.pre_turn(
               ctx.target,
               %{
                 memory_query: %{text: "weather", tier_mode: :short, limit: 5},
                 tool_names: ["weather_lookup"]
               },
               opts
             )

    assert "tool:weather_lookup" in pre_turn.retrieval.tool_tags
    assert pre_turn.retrieval.result_count >= 1

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["tool:weather_lookup"], tier_mode: :short, limit: 5},
               opts
             )

    assert records != []
  end

  defp signal!(type, data) do
    Jido.Signal.new!(type, data, source: "/phase5/test")
  end

  defp memory_runtime_opts(ctx) do
    [
      server: ctx.manager,
      tier: :short,
      store: {Jido.Memory.Store.ETS, [table: ctx.tables.short]}
    ]
  end

  defp memory_runtime_params(ctx, params) do
    params
    |> Map.new()
    |> Map.put(:server, ctx.manager)
    |> Map.put(:tier, :short)
    |> Map.put(:agent_id, ctx.target.id)
    |> Map.put(:store, {Jido.Memory.Store.ETS, [table: ctx.tables.short]})
  end

  defp plugin_context(target, plugin_state) do
    %{
      agent: %{id: target.id, state: %{__memory_os__: plugin_state}},
      plugin_instance: %{state_key: :__memory_os__}
    }
  end

  defp phase5_app_config(tables) do
    %{
      tiers: %{
        short: %{
          store: {Jido.Memory.Store.ETS, [table: tables.short]},
          max_records: 400,
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
        retry_attempts: 1,
        retry_backoff_ms: 10,
        retry_jitter_ms: 5,
        dead_letter_limit: 100,
        consolidation_debounce_ms: 50,
        auto_consolidate: false
      }
    }
  end

  defp cleanup_tables(tables) do
    tables
    |> Map.values()
    |> Enum.each(fn table ->
      for suffix <- ["_records", "_meta"] do
        derived = :"#{table}#{suffix}"
        if :ets.whereis(derived) != :undefined, do: :ets.delete(derived)
      end

      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)
  end
end
