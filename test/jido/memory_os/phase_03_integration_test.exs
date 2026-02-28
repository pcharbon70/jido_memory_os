defmodule Jido.MemoryOS.Phase03IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.MemoryManager

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase3-memory-manager-#{unique}"
    target = %{id: "phase3-agent-#{unique}"}

    tables = %{
      short: :"jido_memory_os_phase3_#{unique}_short",
      mid: :"jido_memory_os_phase3_#{unique}_mid",
      long: :"jido_memory_os_phase3_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!(
      {MemoryManager, name: manager, app_config: phase3_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok, manager: manager, target: target, tables: tables, now: System.system_time(:millisecond)}
  end

  test "retrieve pipeline returns ranked records and explainability details", ctx do
    opts = manager_opts(ctx)

    for index <- 1..3 do
      assert {:ok, _record} =
               Jido.MemoryOS.remember(
                 ctx.target,
                 %{
                   class: :episodic,
                   kind: :event,
                   text: "phase3 retrieval candidate #{index}",
                   content: %{index: index, intent: "rank"},
                   tags: ["persona:ops", "topic:retrieval"],
                   chain_id: "chain:phase3:retrieve",
                   observed_at: ctx.now + index
                 },
                 opts
               )
    end

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tier_mode: :hybrid, text_contains: "candidate", limit: 1, persona_keys: ["ops"]},
               opts
             )

    assert length(records) == 1

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{tier_mode: :hybrid, text_contains: "candidate", limit: 1, persona_keys: ["ops"]},
               opts
             )

    assert explain.scored_candidates != []
    assert explain.excluded != []
    assert explain.selection_rationale.tie_breaker == "final_score desc, observed_at desc, id asc"

    [top | _] = explain.scored_candidates
    assert is_map(top.features)
    assert is_number(top.features.lexical)
    assert is_number(top.features.semantic)
    assert is_number(top.features.recency)
    assert is_number(top.features.heat)
    assert is_number(top.features.persona)
    assert is_number(top.features.tier_bias)
    assert is_number(top.features.final_score)
  end

  test "remember orchestration triggers async consolidation and promotes records", ctx do
    opts = manager_opts(ctx)

    for index <- 1..4 do
      assert {:ok, _record} =
               Jido.MemoryOS.remember(
                 ctx.target,
                 %{
                   class: :episodic,
                   kind: :event,
                   text: "phase3 async consolidate #{index}",
                   content: %{index: index},
                   tags: ["persona:ops", "topic:async"],
                   chain_id: "chain:phase3:async",
                   observed_at: ctx.now + index
                 },
                 opts
               )
    end

    assert wait_until(fn ->
             match?(
               {:ok, records} when records != [],
               Jido.MemoryOS.retrieve(
                 ctx.target,
                 %{kinds: [:page], limit: 20},
                 with_tier(opts, :mid)
               )
             )
           end)

    assert wait_until(fn ->
             match?(
               {:ok, records} when records != [],
               Jido.MemoryOS.retrieve(
                 ctx.target,
                 %{tags_any: ["memory_os:long"], limit: 20},
                 with_tier(opts, :long)
               )
             )
           end)
  end

  test "forget and prune orchestration keeps store consistent", ctx do
    opts = manager_opts(ctx)
    now = System.system_time(:millisecond)

    assert {:ok, _expired} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :semantic,
                 kind: :fact,
                 text: "phase3 expired",
                 tags: ["topic:prune"],
                 expires_at: now - 1
               },
               opts
             )

    assert {:ok, keep} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{class: :semantic, kind: :fact, text: "phase3 keep", tags: ["topic:prune"]},
               opts
             )

    assert {:ok, pruned} = Jido.MemoryOS.prune(ctx.target, opts)
    assert pruned >= 1

    assert {:ok, true} = Jido.MemoryOS.forget(ctx.target, keep.id, opts)
    assert {:ok, false} = Jido.MemoryOS.forget(ctx.target, keep.id, opts)
  end

  test "transient write errors are retried and surfaced in metrics", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase3-flaky-manager-#{unique}"

    tables = %{
      short: :"jido_memory_os_phase3_flaky_#{unique}_short",
      mid: :"jido_memory_os_phase3_flaky_#{unique}_mid",
      long: :"jido_memory_os_phase3_flaky_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!(
      {MemoryManager, name: manager, app_config: phase3_flaky_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    target = %{id: "phase3-flaky-agent-#{unique}"}
    opts = [server: manager]

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "phase3 flaky retry",
                 tags: ["topic:retry"],
                 chain_id: "chain:phase3:retry"
               },
               opts
             )

    assert {:ok, metrics} = MemoryManager.metrics(manager)
    assert metrics.retried >= 1

    assert {:ok, dead_letters} = MemoryManager.dead_letters(manager)
    assert dead_letters == []
  end

  test "queue saturation returns overload diagnostics", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase3-overload-manager-#{unique}"

    tables = %{
      short: :"jido_memory_os_phase3_overload_#{unique}_short",
      mid: :"jido_memory_os_phase3_overload_#{unique}_mid",
      long: :"jido_memory_os_phase3_overload_#{unique}_long"
    }

    cleanup_tables(tables)

    app_config =
      phase3_app_config(tables)
      |> put_in([:manager, :queue_max_depth], 1)
      |> put_in([:manager, :queue_per_agent], 1)
      |> put_in([:manager, :request_timeout_ms], 5_000)

    start_supervised!({MemoryManager, name: manager, app_config: app_config}, id: manager)
    on_exit(fn -> cleanup_tables(tables) end)

    target = %{id: "phase3-overload-agent"}
    base_opts = [server: manager, operation_sleep_ms: 200, call_timeout: :infinity]

    task1 =
      Task.async(fn ->
        Jido.MemoryOS.remember(
          target,
          %{class: :episodic, kind: :event, text: "overload-1", chain_id: "chain:overload"},
          base_opts
        )
      end)

    Process.sleep(20)

    task2 =
      Task.async(fn ->
        Jido.MemoryOS.remember(
          target,
          %{class: :episodic, kind: :event, text: "overload-2", chain_id: "chain:overload"},
          base_opts
        )
      end)

    Process.sleep(20)

    assert {:error, %Jido.Error.ExecutionError{} = error} =
             Jido.MemoryOS.remember(
               target,
               %{class: :episodic, kind: :event, text: "overload-3", chain_id: "chain:overload"},
               server: manager,
               call_timeout: :infinity
             )

    assert get_in(error.details, [:code]) in [:manager_overloaded, :manager_agent_overloaded]
    assert is_integer(get_in(error.details, [:retry_after_ms]))

    assert {:ok, _} = Task.await(task1, 5_000)
    assert {:ok, _} = Task.await(task2, 5_000)
  end

  test "queued request timeout and manager restart remain consistent", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase3-timeout-manager-#{unique}"

    tables = %{
      short: :"jido_memory_os_phase3_timeout_#{unique}_short",
      mid: :"jido_memory_os_phase3_timeout_#{unique}_mid",
      long: :"jido_memory_os_phase3_timeout_#{unique}_long"
    }

    cleanup_tables(tables)

    app_config =
      phase3_app_config(tables)
      |> put_in([:manager, :request_timeout_ms], 5_000)

    start_supervised!({MemoryManager, name: manager, app_config: app_config}, id: manager)
    on_exit(fn -> cleanup_tables(tables) end)

    target = %{id: "phase3-timeout-agent-#{unique}"}

    assert {:error, %Jido.Error.TimeoutError{} = timeout_error} =
             Jido.MemoryOS.remember(
               target,
               %{class: :episodic, kind: :event, text: "timeout-1", chain_id: "chain:timeout"},
               server: manager,
               timeout_ms: 50,
               operation_sleep_ms: 200,
               call_timeout: :infinity
             )

    assert get_in(timeout_error.details, [:code]) == :manager_timeout

    pid_before = Process.whereis(manager)
    assert is_pid(pid_before)
    Process.exit(pid_before, :kill)

    assert wait_until(fn ->
             case Process.whereis(manager) do
               pid when is_pid(pid) -> pid != pid_before
               _ -> false
             end
           end)

    assert {:ok, first} =
             Jido.MemoryOS.remember(
               target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "restart-stability",
                 tags: ["topic:restart"],
                 chain_id: "chain:phase3:restart"
               },
               server: manager
             )

    assert {:ok, second} =
             Jido.MemoryOS.remember(
               target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "restart-stability",
                 tags: ["topic:restart"],
                 chain_id: "chain:phase3:restart"
               },
               server: manager
             )

    assert first.id == second.id

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               target,
               %{text_contains: "restart-stability", limit: 20},
               server: manager,
               tier: :short
             )

    assert Enum.count(records, &(&1.id == first.id)) == 1
  end

  defp manager_opts(ctx), do: [server: ctx.manager]
  defp with_tier(opts, tier), do: Keyword.put(opts, :tier, tier)

  defp wait_until(fun, timeout_ms \\ 2_000, step_ms \\ 25) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, step_ms)
  end

  defp do_wait_until(fun, deadline, step_ms) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(step_ms)
        do_wait_until(fun, deadline, step_ms)
      end
    end
  end

  defp phase3_app_config(tables) do
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
        auto_consolidate: true
      }
    }
  end

  defp phase3_flaky_app_config(tables) do
    phase3_app_config(tables)
    |> put_in(
      [:tiers, :short, :store],
      {__MODULE__.FlakyStore, [table: tables.short, fail_puts: 1]}
    )
  end

  defp cleanup_tables(tables) do
    Enum.each(Map.values(tables), &cleanup_store_tables/1)
  end

  defp cleanup_store_tables(base) do
    counter = :"#{base}_flaky_counter"

    Enum.each(
      [
        base,
        :"#{base}_records",
        :"#{base}_ns_time",
        :"#{base}_ns_class_time",
        :"#{base}_ns_tag",
        counter
      ],
      fn table ->
        if :ets.whereis(table) != :undefined do
          :ets.delete(table)
        end
      end
    )
  end

  defmodule FlakyStore do
    @moduledoc false
    @behaviour Jido.Memory.Store

    @impl true
    def ensure_ready(opts) do
      ensure_counter_table(opts)
      Jido.Memory.Store.ETS.ensure_ready(strip_opts(opts))
    end

    @impl true
    def put(record, opts) do
      ensure_counter_table(opts)
      fail_puts = Keyword.get(opts, :fail_puts, 0)
      counter = counter_table(opts)
      attempts = :ets.update_counter(counter, :put_attempts, {2, 1}, {:put_attempts, 0})

      if attempts <= fail_puts do
        {:error, {:put_failed, :transient_simulated}}
      else
        Jido.Memory.Store.ETS.put(record, strip_opts(opts))
      end
    end

    @impl true
    def get(key, opts), do: Jido.Memory.Store.ETS.get(key, strip_opts(opts))

    @impl true
    def delete(key, opts), do: Jido.Memory.Store.ETS.delete(key, strip_opts(opts))

    @impl true
    def query(query, opts), do: Jido.Memory.Store.ETS.query(query, strip_opts(opts))

    @impl true
    def prune_expired(opts), do: Jido.Memory.Store.ETS.prune_expired(strip_opts(opts))

    defp strip_opts(opts), do: Keyword.drop(opts, [:fail_puts])

    defp ensure_counter_table(opts) do
      counter = counter_table(opts)

      if :ets.whereis(counter) == :undefined do
        :ets.new(counter, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])
      end

      :ok
    rescue
      ArgumentError -> :ok
    end

    defp counter_table(opts) do
      base =
        case Keyword.get(opts, :table, :jido_memory_os_phase3_flaky) do
          atom when is_atom(atom) -> atom
          string when is_binary(string) -> String.to_atom(string)
        end

      :"#{base}_flaky_counter"
    end
  end
end
