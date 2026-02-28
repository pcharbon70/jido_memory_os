defmodule Jido.MemoryOS.CrossPhaseAcceptanceTest do
  use ExUnit.Case, async: false

  alias Jido.Memory.Runtime

  alias Jido.MemoryOS.{
    MemoryManager,
    Migration,
    Performance,
    ReleaseController
  }

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"cross-phase-memory-manager-#{unique}"

    tables = %{
      short: :"jido_memory_os_cross_#{unique}_short",
      mid: :"jido_memory_os_cross_#{unique}_mid",
      long: :"jido_memory_os_cross_#{unique}_long",
      legacy: :"jido_memory_os_cross_#{unique}_legacy"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: base_app_config(tables)},
      id: manager
    )

    target = %{id: "cross-agent-#{unique}", group: "group-alpha"}

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok,
     unique: unique,
     manager: manager,
     tables: tables,
     target: target,
     legacy_store: {Jido.Memory.Store.ETS, [table: tables.legacy]},
     now: System.system_time(:millisecond)}
  end

  test "X-1 long multi-session persona recall remains consistent after multiple consolidations",
       ctx do
    opts = actor_opts(ctx)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "session 1: user prefers concise answers",
                 tags: ["persona:style", "topic:response", "fact_key:user.style.answer_length"],
                 chain_id: "chain:cross:x1:1",
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
                 text: "session 2: keep responses concise and practical",
                 tags: ["persona:style", "topic:response", "fact_key:user.style.answer_length"],
                 chain_id: "chain:cross:x1:2",
                 observed_at: ctx.now + 2
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
                 text: "session 3: unrelated note about weather",
                 tags: ["topic:weather"],
                 chain_id: "chain:cross:x1:3",
                 observed_at: ctx.now + 3
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
                 text_contains: "concise",
                 limit: 5
               },
               opts
             )

    assert records != []
    assert Enum.any?(records, &String.contains?(String.downcase(&1.text || ""), "concise"))
  end

  test "X-2 contradictory user facts are deterministic with explainable lineage", ctx do
    opts = actor_opts(ctx)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user tone preference is friendly",
                 tags: ["fact_key:user.preference.tone", "persona:style", "topic:profile"],
                 chain_id: "chain:cross:x2",
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
                 text: "user tone preference is formal",
                 tags: ["fact_key:user.preference.tone", "persona:style", "topic:profile"],
                 chain_id: "chain:cross:x2",
                 observed_at: ctx.now + 20
               },
               opts
             )

    assert {:ok, _summary} = Jido.MemoryOS.consolidate(ctx.target, opts)

    query = %{
      tier_mode: :long,
      tags_any: ["fact_key:user.preference.tone"],
      text_contains: "tone preference",
      limit: 5
    }

    assert {:ok, records} = Jido.MemoryOS.retrieve(ctx.target, query, opts)
    assert length(records) >= 1
    assert String.contains?(String.downcase(hd(records).text || ""), "formal")

    assert {:ok, explain} = Jido.MemoryOS.explain_retrieval(ctx.target, query, opts)
    assert explain.recent_conflicts != []
    assert Enum.any?(explain.recent_conflicts, &(&1.fact_key == "user.preference.tone"))
    assert Enum.any?(explain.recent_conflicts, &(&1.strategy == :version))
  end

  test "X-3 concurrent mixed workloads satisfy baseline latency and throughput SLOs", ctx do
    slos = Performance.default_slos()

    result =
      Performance.benchmark_mixed(ctx.target, 24, 6,
        server: ctx.manager,
        actor_id: ctx.target.id,
        actor_group: ctx.target.group,
        target_group: ctx.target.group
      )

    assert result.errors == 0
    assert result.throughput_per_sec >= slos.throughput_per_sec.mixed
    assert result.p95_ms <= slos.latency_ms.mixed.p95
  end

  test "X-4 unauthorized cross-agent access is blocked and audited", ctx do
    opts = actor_opts(ctx)

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "protected note",
                 chain_id: "chain:cross:x4"
               },
               opts
             )

    assert {:error, %Jido.Error.ExecutionError{} = error} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "protected", limit: 1},
               server: ctx.manager,
               actor_id: "intruder",
               actor_group: "group-beta"
             )

    assert get_in(error.details, [:code]) == :access_denied

    assert {:ok, audits} = MemoryManager.audit_events(ctx.manager, limit: 100)

    assert Enum.any?(audits, fn event ->
             event.category == :access and event.action == :retrieve and event.outcome == :deny
           end)
  end

  test "X-5 sensitive memory is protected at write-time and retrieval-time", _ctx do
    unique = System.unique_integer([:positive, :monotonic])

    redaction_manager = :"cross-x5-redaction-manager-#{unique}"

    redaction_tables = %{
      short: :"jido_memory_os_cross_x5a_#{unique}_short",
      mid: :"jido_memory_os_cross_x5a_#{unique}_mid",
      long: :"jido_memory_os_cross_x5a_#{unique}_long",
      legacy: :"jido_memory_os_cross_x5a_#{unique}_legacy"
    }

    cleanup_tables(redaction_tables)

    redaction_config =
      base_app_config(redaction_tables)
      |> put_in([:safety, :redaction_enabled], true)
      |> put_in([:safety, :pii_strategy], :mask)

    start_supervised!({MemoryManager, name: redaction_manager, app_config: redaction_config},
      id: redaction_manager
    )

    on_exit(fn -> cleanup_tables(redaction_tables) end)

    target = %{id: "cross-x5-redaction-agent-#{unique}", group: "group-alpha"}

    raw_text = "email me at pii@example.com and call 415-555-1212"

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: raw_text,
                 chain_id: "chain:cross:x5:redact"
               },
               server: redaction_manager,
               actor_id: target.id,
               actor_group: target.group,
               target_group: target.group
             )

    assert {:ok, [stored | _]} =
             Jido.MemoryOS.retrieve(
               target,
               %{text_contains: "email me", limit: 1},
               server: redaction_manager,
               actor_id: target.id,
               actor_group: target.group,
               actor_role: "admin",
               target_group: target.group
             )

    assert stored.text =~ "[REDACTED_EMAIL]"
    assert stored.text =~ "[REDACTED_PHONE]"
    refute stored.text =~ "pii@example.com"

    masking_manager = :"cross-x5-masking-manager-#{unique}"

    masking_tables = %{
      short: :"jido_memory_os_cross_x5b_#{unique}_short",
      mid: :"jido_memory_os_cross_x5b_#{unique}_mid",
      long: :"jido_memory_os_cross_x5b_#{unique}_long",
      legacy: :"jido_memory_os_cross_x5b_#{unique}_legacy"
    }

    cleanup_tables(masking_tables)

    masking_config =
      base_app_config(masking_tables)
      |> put_in([:safety, :redaction_enabled], false)

    start_supervised!({MemoryManager, name: masking_manager, app_config: masking_config},
      id: masking_manager
    )

    on_exit(fn -> cleanup_tables(masking_tables) end)

    masking_target = %{id: "cross-x5-masking-agent-#{unique}", group: "group-alpha"}

    assert {:ok, _} =
             Jido.MemoryOS.remember(
               masking_target,
               %{class: :episodic, kind: :event, text: raw_text, chain_id: "chain:cross:x5:mask"},
               server: masking_manager,
               actor_id: masking_target.id,
               actor_group: masking_target.group,
               target_group: masking_target.group
             )

    assert {:ok, [masked | _]} =
             Jido.MemoryOS.retrieve(
               masking_target,
               %{text_contains: "email me", limit: 1},
               server: masking_manager,
               actor_id: masking_target.id,
               actor_group: masking_target.group,
               actor_role: "reviewer",
               target_group: masking_target.group
             )

    assert masked.text =~ "[REDACTED_EMAIL]"
    assert masked.text =~ "[REDACTED_PHONE]"

    assert {:ok, [admin | _]} =
             Jido.MemoryOS.retrieve(
               masking_target,
               %{text_contains: "email me", limit: 1},
               server: masking_manager,
               actor_id: masking_target.id,
               actor_group: masking_target.group,
               actor_role: "admin",
               target_group: masking_target.group
             )

    assert admin.text =~ "pii@example.com"
    assert admin.text =~ "415-555-1212"
  end

  test "X-6 crash/restart during consolidation recovers without duplicate or lost updates", ctx do
    ensure_persistent_tier_tables(ctx.tables)
    opts = actor_opts(ctx, call_timeout: :infinity)

    inserted_ids =
      for index <- 1..6 do
        assert {:ok, record} =
                 Jido.MemoryOS.remember(
                   ctx.target,
                   %{
                     class: :episodic,
                     kind: :event,
                     text: "cross x6 durable fact #{index}",
                     tags: ["topic:recovery", "fact_key:user.recovery.preference"],
                     chain_id: "chain:cross:x6",
                     observed_at: ctx.now + index
                   },
                   opts
                 )

        record.id
      end

    consolidate_task =
      Task.Supervisor.async_nolink(Jido.MemoryOS.ManagerWorkers, fn ->
        Jido.MemoryOS.consolidate(ctx.target, opts)
      end)

    Process.sleep(40)

    pid_before = Process.whereis(ctx.manager)
    Process.exit(pid_before, :kill)

    _ = Task.yield(consolidate_task, 100) || Task.shutdown(consolidate_task, :brutal_kill)

    assert wait_until(fn ->
             case Process.whereis(ctx.manager) do
               nil -> false
               new_pid -> new_pid != pid_before
             end
           end)

    assert {:ok, _} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, short_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["fact_key:user.recovery.preference"], limit: 50},
               opts ++ [tier: :short, tier_mode: :short]
             )

    assert {:ok, mid_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["fact_key:user.recovery.preference"], limit: 50},
               opts ++ [tier: :mid, tier_mode: :mid]
             )

    assert {:ok, long_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["fact_key:user.recovery.preference"], limit: 50},
               opts ++ [tier: :long, tier_mode: :long]
             )

    all_records = short_records ++ mid_records ++ long_records

    assert all_records != []
    assert Enum.map(short_records, & &1.id) == Enum.uniq(Enum.map(short_records, & &1.id))
    assert Enum.map(mid_records, & &1.id) == Enum.uniq(Enum.map(mid_records, & &1.id))
    assert Enum.map(long_records, & &1.id) == Enum.uniq(Enum.map(long_records, & &1.id))

    covered_source_ids =
      all_records
      |> Enum.reduce(MapSet.new(), fn record, acc ->
        from_self = MapSet.put(acc, record.id)

        from_content =
          record
          |> extract_source_short_ids()
          |> Enum.reduce(from_self, &MapSet.put(&2, &1))

        from_content
      end)

    assert MapSet.subset?(MapSet.new(inserted_ids), covered_source_ids)
  end

  test "X-7 migration from legacy records preserves retrieval quality and integrity", ctx do
    seed_legacy(ctx, ctx.target,
      id: "x7-a",
      class: :episodic,
      text: "migration profile preference alpha",
      tags: ["topic:migration", "persona:profile"]
    )

    seed_legacy(ctx, ctx.target,
      id: "x7-b",
      class: :semantic,
      text: "migration profile preference beta",
      tags: ["topic:migration", "persona:profile"]
    )

    seed_legacy(ctx, ctx.target,
      id: "x7-c",
      class: :working,
      text: "migration profile preference gamma",
      tags: ["topic:migration", "persona:profile"]
    )

    assert {:ok, report} =
             Migration.migrate_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id,
               actor_group: ctx.target.group,
               target_group: ctx.target.group,
               on_error: :halt
             )

    assert report.source_count == 3
    assert report.migrated_count == 3
    assert report.failed_count == 0

    assert {:ok, reconciliation} =
             Migration.reconcile_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id,
               actor_group: ctx.target.group,
               target_group: ctx.target.group
             )

    assert reconciliation.parity?

    assert {:ok, legacy_records} =
             Runtime.recall(ctx.target, %{
               namespace: legacy_namespace(ctx.target),
               store: ctx.legacy_store,
               text_contains: "migration profile preference",
               limit: 20
             })

    assert {:ok, memory_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tier_mode: :all, text_contains: "migration profile preference", limit: 20},
               actor_opts(ctx)
             )

    legacy_texts = MapSet.new(Enum.map(legacy_records, &String.downcase(&1.text || "")))
    memory_texts = MapSet.new(Enum.map(memory_records, &String.downcase(&1.text || "")))

    assert MapSet.subset?(legacy_texts, memory_texts)
  end

  test "X-8 canary rollout with fallback and rollback is predictable during incident simulation",
       ctx do
    external_target = %{id: "cross-x8-agent-#{ctx.unique}", group: "external"}

    seed_legacy(ctx, external_target,
      id: "x8-legacy",
      class: :episodic,
      text: "x8 fallback record",
      tags: ["topic:x8"]
    )

    controller = :"cross-x8-controller-#{ctx.unique}"

    start_supervised!(
      {ReleaseController,
       name: controller,
       mode: :memory_os,
       rollout_stage: :canary,
       canary_ratio: 1.0,
       internal_groups: ["internal"],
       fallback_enabled: true,
       memory_server: :missing_x8_memory_manager,
       legacy_store: ctx.legacy_store,
       rollback_thresholds: %{latency_p95_ms: 120, error_rate: 0.02, queue_depth: 30}},
      id: controller
    )

    assert {:ok, records} =
             ReleaseController.retrieve(
               controller,
               external_target,
               %{text_contains: "x8 fallback", limit: 5},
               actor_id: external_target.id
             )

    assert Enum.any?(records, &(&1.id == "x8-legacy"))

    assert {:ok, status_before} = ReleaseController.status(controller)
    assert status_before.metrics.fallbacks >= 1

    assert {:ok, {:rollback, reason}} =
             ReleaseController.evaluate_rollback(controller, %{
               latency_p95_ms: 400,
               error_rate: 0.01,
               queue_depth: 10
             })

    assert reason.mode_before == :memory_os
    assert reason.mode_after == :legacy

    assert {:ok, :legacy} = ReleaseController.mode(controller)

    assert {:ok, records_after} =
             ReleaseController.retrieve(
               controller,
               external_target,
               %{text_contains: "x8 fallback", limit: 5},
               actor_id: external_target.id
             )

    assert Enum.any?(records_after, &(&1.id == "x8-legacy"))
  end

  defp actor_opts(ctx, extra \\ []) do
    [
      server: ctx.manager,
      actor_id: ctx.target.id,
      actor_group: ctx.target.group,
      target_group: ctx.target.group
    ] ++ extra
  end

  defp seed_legacy(ctx, target, attrs) do
    now = System.system_time(:millisecond)

    defaults = %{
      class: :episodic,
      kind: :event,
      observed_at: now,
      metadata: %{}
    }

    payload = defaults |> Map.merge(Map.new(attrs))

    assert {:ok, _} =
             Runtime.remember(target, payload,
               namespace: legacy_namespace(target),
               store: ctx.legacy_store
             )
  end

  defp legacy_namespace(target), do: "agent:" <> target.id

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

  defp extract_source_short_ids(record) do
    content = record.content || %{}
    provenance = map_get_map(content, :provenance)

    direct_ids = normalize_string_list(map_get_list(content, :source_short_ids))
    provenance_ids = normalize_string_list(map_get_list(provenance, :source_short_ids))

    (direct_ids ++ provenance_ids)
    |> Enum.uniq()
  end

  defp base_app_config(tables) do
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
      lifecycle: %{
        segment_max_events: 2,
        segment_max_tokens: 256,
        page_max_segments: 1,
        conflict_strategy: :version,
        promotion_min_score: 0.5
      },
      safety: %{
        audit_enabled: true,
        redaction_enabled: false,
        pii_strategy: :mask
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
        adaptive_throttle_enabled: true,
        adaptive_throttle_target_depth: 180,
        adaptive_throttle_soft_limit: 0.8,
        query_cache_enabled: true,
        query_cache_ttl_ms: 800,
        query_cache_max_entries: 512,
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
        masking: %{
          default_mode: :allow,
          role_modes: %{admin: :allow, reviewer: :mask, external: :drop}
        }
      }
    }
  end

  defp cleanup_tables(tables) do
    tables
    |> Map.values()
    |> Enum.each(fn table ->
      for suffix <- [
            "_records",
            "_meta",
            "_ns_time",
            "_ns_class_time",
            "_ns_tag",
            "_flaky_counter"
          ] do
        derived = :"#{table}#{suffix}"
        if :ets.whereis(derived) != :undefined, do: :ets.delete(derived)
      end

      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)
  end

  defp ensure_persistent_tier_tables(tables) do
    for table <- [tables.short, tables.mid, tables.long] do
      :ok = Jido.Memory.Store.ETS.ensure_ready(table: table)
    end

    :ok
  end

  defp map_get_list(map, key) when is_map(map) and is_atom(key) do
    map
    |> Map.get(key, Map.get(map, Atom.to_string(key), []))
    |> case do
      value when is_list(value) -> value
      _ -> []
    end
  end

  defp map_get_list(_map, _key), do: []

  defp map_get_map(map, key) when is_map(map) and is_atom(key) do
    map
    |> Map.get(key, Map.get(map, Atom.to_string(key), %{}))
    |> case do
      value when is_map(value) -> value
      _ -> %{}
    end
  end

  defp map_get_map(_map, _key), do: %{}

  defp normalize_string_list(values) when is_list(values) do
    values
    |> Enum.map(fn
      value when is_binary(value) -> String.trim(value)
      value when is_atom(value) -> value |> Atom.to_string() |> String.trim()
      value -> value |> to_string() |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
  end

  defp normalize_string_list(_), do: []
end
