defmodule Jido.MemoryOS.Phase07IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.{MemoryManager, Performance}

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase7-memory-manager-#{unique}"
    journal_path = Path.join(System.tmp_dir!(), "jido_memory_os_phase7_#{unique}.journal")

    target = %{id: "phase7-agent-#{unique}", group: "group-phase7"}

    tables = %{
      short: :"jido_memory_os_phase7_#{unique}_short",
      mid: :"jido_memory_os_phase7_#{unique}_mid",
      long: :"jido_memory_os_phase7_#{unique}_long"
    }

    cleanup_tables(tables)
    cleanup_file(journal_path)

    start_supervised!(
      {MemoryManager, name: manager, app_config: phase7_app_config(tables, journal_path)},
      id: manager
    )

    on_exit(fn ->
      cleanup_tables(tables)
      cleanup_file(journal_path)
    end)

    {:ok,
     manager: manager, target: target, tables: tables, journal_path: journal_path, unique: unique}
  end

  test "baseline concurrency benchmark meets locked SLOs", ctx do
    opts = [server: ctx.manager, actor_id: ctx.target.id]
    slos = Performance.default_slos()

    ingest = Performance.benchmark_ingestion(ctx.target, 24, 6, opts)
    retrieve = Performance.benchmark_retrieval(ctx.target, 24, 6, opts)
    mixed = Performance.benchmark_mixed(ctx.target, 24, 6, opts)

    assert ingest.errors == 0
    assert retrieve.errors == 0
    assert mixed.errors == 0

    assert ingest.throughput_per_sec >= slos.throughput_per_sec.remember
    assert retrieve.throughput_per_sec >= slos.throughput_per_sec.retrieve
    assert mixed.throughput_per_sec >= slos.throughput_per_sec.mixed

    assert ingest.p95_ms <= slos.latency_ms.remember.p95
    assert retrieve.p95_ms <= slos.latency_ms.retrieve.p95
    assert mixed.p95_ms <= slos.latency_ms.mixed.p95
  end

  test "adaptive throttling degrades gracefully under overload", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase7-overload-manager-#{unique}"

    journal_path =
      Path.join(System.tmp_dir!(), "jido_memory_os_phase7_overload_#{unique}.journal")

    tables = %{
      short: :"jido_memory_os_phase7_overload_#{unique}_short",
      mid: :"jido_memory_os_phase7_overload_#{unique}_mid",
      long: :"jido_memory_os_phase7_overload_#{unique}_long"
    }

    cleanup_tables(tables)
    cleanup_file(journal_path)

    app_config =
      phase7_app_config(tables, journal_path)
      |> put_in([:manager, :queue_max_depth], 6)
      |> put_in([:manager, :queue_per_agent], 6)
      |> put_in([:manager, :adaptive_throttle_enabled], true)
      |> put_in([:manager, :adaptive_throttle_target_depth], 3)
      |> put_in([:manager, :adaptive_throttle_soft_limit], 0.5)

    start_supervised!({MemoryManager, name: manager, app_config: app_config}, id: manager)

    on_exit(fn ->
      cleanup_tables(tables)
      cleanup_file(journal_path)
    end)

    target = %{id: "phase7-overload-agent-#{unique}"}

    opts = [
      server: manager,
      actor_id: target.id,
      operation_sleep_ms: 120,
      call_timeout: :infinity
    ]

    tasks =
      Enum.map(1..14, fn idx ->
        Task.async(fn ->
          Jido.MemoryOS.remember(
            target,
            %{
              class: :episodic,
              kind: :event,
              text: "overload #{idx}",
              chain_id: "chain:phase7:overload"
            },
            opts
          )
        end)
      end)

    results = Enum.map(tasks, &Task.await(&1, 10_000))

    assert Enum.any?(results, fn
             {:error, %Jido.Error.ExecutionError{details: details}} ->
               Map.get(details, :code) in [
                 :manager_throttled,
                 :manager_agent_throttled,
                 :manager_overloaded,
                 :manager_agent_overloaded
               ]

             _ ->
               false
           end)

    assert {:ok, _records} =
             Jido.MemoryOS.retrieve(target, %{limit: 5}, server: manager, actor_id: target.id)

    assert {:ok, metrics} = MemoryManager.metrics(manager)
    assert metrics.throttled > 0 or metrics.overloaded > 0
  end

  test "weighted scheduler fairness prevents starvation", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase7-fairness-manager-#{unique}"

    journal_path =
      Path.join(System.tmp_dir!(), "jido_memory_os_phase7_fairness_#{unique}.journal")

    tables = %{
      short: :"jido_memory_os_phase7_fairness_#{unique}_short",
      mid: :"jido_memory_os_phase7_fairness_#{unique}_mid",
      long: :"jido_memory_os_phase7_fairness_#{unique}_long"
    }

    cleanup_tables(tables)
    cleanup_file(journal_path)

    app_config =
      phase7_app_config(tables, journal_path)
      |> put_in([:manager, :scheduler_strategy], :weighted_priority)
      |> put_in([:manager, :weighted_priority_bias], 12)
      |> put_in([:manager, :fairness_max_wait_ms], 20)
      |> put_in([:manager, :queue_max_depth], 128)
      |> put_in([:manager, :queue_per_agent], 128)

    start_supervised!({MemoryManager, name: manager, app_config: app_config}, id: manager)

    on_exit(fn ->
      cleanup_tables(tables)
      cleanup_file(journal_path)
    end)

    target_a = %{id: "phase7-fair-a-#{unique}"}
    target_b = %{id: "phase7-fair-b-#{unique}"}

    opts_a = [
      server: manager,
      actor_id: target_a.id,
      operation_sleep_ms: 25,
      call_timeout: :infinity
    ]

    opts_b = [
      server: manager,
      actor_id: target_b.id,
      operation_sleep_ms: 25,
      call_timeout: :infinity
    ]

    heavy_a =
      Enum.map(1..18, fn idx ->
        Task.async(fn ->
          Jido.MemoryOS.remember(
            target_a,
            %{class: :episodic, kind: :event, text: "A#{idx}", chain_id: "chain:phase7:fair:a"},
            opts_a
          )
        end)
      end)

    Process.sleep(35)

    b_task =
      Task.async(fn ->
        Jido.MemoryOS.remember(
          target_b,
          %{class: :episodic, kind: :event, text: "B-priority", chain_id: "chain:phase7:fair:b"},
          opts_b
        )
      end)

    Enum.each(heavy_a, &Task.await(&1, 10_000))
    assert {:ok, _record} = Task.await(b_task, 10_000)

    assert {:ok, metrics} = MemoryManager.metrics(manager)
    assert metrics.starvation_prevented >= 1
  end

  test "restart with in-flight replay-safe operation avoids duplicate side effects", ctx do
    idempotency_key = "phase7-replay-#{ctx.unique}"

    opts =
      [
        server: ctx.manager,
        actor_id: ctx.target.id,
        replay_safe: true,
        idempotency_key: idempotency_key,
        operation_sleep_ms: 300,
        call_timeout: :infinity
      ]

    remember_task =
      Task.Supervisor.async_nolink(Jido.MemoryOS.ManagerWorkers, fn ->
        Jido.MemoryOS.remember(
          ctx.target,
          %{
            class: :episodic,
            kind: :event,
            text: "replay-safe remember",
            chain_id: "chain:phase7:replay"
          },
          opts
        )
      end)

    Process.sleep(70)
    pid = Process.whereis(ctx.manager)
    Process.exit(pid, :kill)
    _ = Task.yield(remember_task, 100) || Task.shutdown(remember_task, :brutal_kill)

    assert wait_until(fn ->
             case Process.whereis(ctx.manager) do
               nil -> false
               new_pid -> new_pid != pid
             end
           end)

    assert wait_until(fn ->
             case Jido.MemoryOS.retrieve(
                    ctx.target,
                    %{text_contains: "replay-safe remember", limit: 10},
                    server: ctx.manager,
                    actor_id: ctx.target.id
                  ) do
               {:ok, records} -> length(records) == 1
               _ -> false
             end
           end)

    assert {:ok, [stored]} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "replay-safe remember", limit: 10},
               server: ctx.manager,
               actor_id: ctx.target.id
             )

    assert {:ok, replayed} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "replay-safe remember",
                 chain_id: "chain:phase7:replay"
               },
               server: ctx.manager,
               actor_id: ctx.target.id,
               idempotency_key: idempotency_key
             )

    assert replayed.id == stored.id

    assert {:ok, events} = MemoryManager.journal_events(ctx.manager, limit: 200)
    assert Enum.any?(events, &(Map.get(&1, :status) == :idempotent_reuse))
  end

  test "journal replay metadata remains complete and deterministic", ctx do
    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "journal integrity",
                 chain_id: "chain:phase7:journal"
               },
               server: ctx.manager,
               actor_id: ctx.target.id,
               replay_safe: true,
               idempotency_key: "journal-#{ctx.unique}"
             )

    assert {:ok, events} = MemoryManager.journal_events(ctx.manager, limit: 200)
    assert events != []

    assert Enum.all?(events, fn event ->
             is_integer(Map.get(event, :seq)) and is_integer(Map.get(event, :at)) and
               Map.get(event, :status) != nil
           end)

    grouped = Enum.group_by(events, &Map.get(&1, :request_id))

    assert Enum.any?(grouped, fn {_request_id, request_events} ->
             statuses = Enum.map(request_events, &Map.get(&1, :status))
             :queued in statuses and :started in statuses and :completed in statuses
           end)
  end

  test "post-fault SLO recovery returns to healthy throughput", ctx do
    opts = [
      server: ctx.manager,
      actor_id: ctx.target.id,
      operation_sleep_ms: 60,
      timeout_ms: 30,
      call_timeout: :infinity
    ]

    storm =
      Enum.map(1..10, fn idx ->
        Task.async(fn ->
          Jido.MemoryOS.remember(
            ctx.target,
            %{
              class: :episodic,
              kind: :event,
              text: "fault storm #{idx}",
              chain_id: "chain:phase7:storm"
            },
            opts
          )
        end)
      end)

    _ = Enum.map(storm, &Task.await(&1, 10_000))

    assert wait_until(fn ->
             case MemoryManager.metrics(ctx.manager) do
               {:ok, metrics} -> metrics.queue_depth == 0
               _ -> false
             end
           end)

    slos = Performance.default_slos()

    recovery =
      Performance.benchmark_ingestion(ctx.target, 16, 4,
        server: ctx.manager,
        actor_id: ctx.target.id
      )

    assert recovery.throughput_per_sec >= slos.throughput_per_sec.remember
    assert recovery.p95_ms <= slos.latency_ms.remember.p95

    transient_rate = recovery.errors / max(1, recovery.count)
    assert transient_rate <= slos.error_budget.transient_rate_max
  end

  defp phase7_app_config(tables, journal_path) do
    %{
      tiers: %{
        short: %{
          store: {Jido.Memory.Store.ETS, [table: tables.short]},
          max_records: 600,
          ttl_ms: 3_600_000,
          promotion_threshold: 0.2
        },
        mid: %{
          store: {Jido.Memory.Store.ETS, [table: tables.mid]},
          max_records: 3_000,
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
      manager: %{
        queue_max_depth: 256,
        queue_per_agent: 64,
        request_timeout_ms: 3_000,
        retry_attempts: 1,
        retry_backoff_ms: 10,
        retry_jitter_ms: 5,
        dead_letter_limit: 200,
        consolidation_debounce_ms: 40,
        auto_consolidate: false,
        scheduler_strategy: :round_robin,
        weighted_priority_bias: 3,
        fairness_max_wait_ms: 80,
        adaptive_throttle_enabled: true,
        adaptive_throttle_target_depth: 180,
        adaptive_throttle_soft_limit: 0.8,
        query_cache_enabled: true,
        query_cache_ttl_ms: 800,
        query_cache_max_entries: 512,
        journal_path: journal_path,
        journal_limit: 2_000,
        replay_on_start: true
      },
      governance: %{
        policy: %{default_effect: :deny, allow_same_group: true, rules: []},
        approvals: %{
          enabled: true,
          required_actions: [],
          ttl_ms: 60_000,
          max_tokens: 200,
          one_time: true
        },
        audit: %{enabled: true, max_events: 1_000},
        retention: %{
          short: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []},
          mid: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []},
          long: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []}
        },
        masking: %{default_mode: :allow, role_modes: %{admin: :allow}}
      }
    }
  end

  defp wait_until(fun, attempts \\ 80)

  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(30)
      wait_until(fun, attempts - 1)
    end
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

  defp cleanup_file(path) when is_binary(path) do
    if File.exists?(path), do: File.rm(path)
  end
end
