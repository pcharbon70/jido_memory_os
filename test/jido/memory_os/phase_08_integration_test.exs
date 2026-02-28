defmodule Jido.MemoryOS.Phase08IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.Memory.Runtime
  alias Jido.MemoryOS.{MemoryManager, Metadata, Migration, ReleaseController}

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase8-memory-manager-#{unique}"

    target = %{id: "phase8-agent-#{unique}", group: "internal"}

    tables = %{
      short: :"jido_memory_os_phase8_#{unique}_short",
      mid: :"jido_memory_os_phase8_#{unique}_mid",
      long: :"jido_memory_os_phase8_#{unique}_long",
      legacy: :"jido_memory_os_phase8_#{unique}_legacy"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: phase8_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok,
     manager: manager,
     target: target,
     tables: tables,
     legacy_store: {Jido.Memory.Store.ETS, [table: tables.legacy]},
     unique: unique}
  end

  test "legacy migration classifies tiers, backfills metadata, and reconciles counts", ctx do
    seed_legacy(ctx, %{id: "phase8-short", class: :working, tags: ["short_term"], text: "short"})

    seed_legacy(ctx, %{
      id: "phase8-mid",
      class: :episodic,
      tags: ["mid_term", "persona:owner"],
      text: String.duplicate("m", 240)
    })

    seed_legacy(ctx, %{id: "phase8-long", class: :semantic, tags: ["long_term"], text: "long"})

    assert {:ok, report} =
             Migration.migrate_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id,
               on_error: :halt
             )

    assert report.source_count == 3
    assert report.migrated_count == 3
    assert report.failed_count == 0
    assert report.per_tier.short == 1
    assert report.per_tier.mid == 1
    assert report.per_tier.long == 1

    assert {:ok, reconciliation} =
             Migration.reconcile_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id
             )

    assert reconciliation.legacy_count == 3
    assert reconciliation.memory_os_count == 3
    assert reconciliation.parity?

    assert {:ok, [short]} = retrieve_tier(ctx, :short, "short")
    assert {:ok, [mid]} = retrieve_tier(ctx, :mid, "mmmm")
    assert {:ok, [long]} = retrieve_tier(ctx, :long, "long")

    assert {:ok, short_meta} = Metadata.from_record(short)
    assert {:ok, mid_meta} = Metadata.from_record(mid)
    assert {:ok, long_meta} = Metadata.from_record(long)

    assert short_meta.tier == :short
    assert mid_meta.tier == :mid
    assert long_meta.tier == :long

    assert "owner" in mid_meta.persona_keys
  end

  test "dual-run drift threshold blocks cutover when parity is poor", ctx do
    controller =
      start_release_controller!(ctx,
        mode: :dual_run,
        rollout_stage: :global,
        drift_threshold: 0.01,
        drift_min_samples: 2
      )

    seed_legacy(ctx, %{id: "phase8-legacy-only", class: :episodic, text: "legacy only"})

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{id: "phase8-memory-only", class: :episodic, kind: :event, text: "memory only"},
               server: ctx.manager,
               actor_id: ctx.target.id,
               tier: :short
             )

    assert {:ok, legacy_records} =
             ReleaseController.retrieve(controller, ctx.target, %{limit: 20},
               actor_id: ctx.target.id
             )

    assert Enum.any?(legacy_records, &(&1.id == "phase8-legacy-only"))

    assert {:ok, _legacy_records} =
             ReleaseController.retrieve(controller, ctx.target, %{limit: 20},
               actor_id: ctx.target.id
             )

    assert {:ok, false} = ReleaseController.cutover_ready?(controller)

    assert {:error, {:cutover_blocked, summary}} =
             ReleaseController.set_mode(controller, :memory_os)

    assert summary.samples >= 2
    assert summary.over_threshold >= 1
  end

  test "cutover succeeds after healthy dual-run drift samples", ctx do
    controller =
      start_release_controller!(ctx,
        mode: :dual_run,
        rollout_stage: :global,
        drift_threshold: 0.25,
        drift_min_samples: 2
      )

    assert {:ok, _record} =
             ReleaseController.remember(
               controller,
               ctx.target,
               %{id: "phase8-cutover-1", class: :episodic, kind: :event, text: "cutover one"},
               actor_id: ctx.target.id
             )

    assert {:ok, _record} =
             ReleaseController.remember(
               controller,
               ctx.target,
               %{id: "phase8-cutover-2", class: :episodic, kind: :event, text: "cutover two"},
               actor_id: ctx.target.id
             )

    assert {:ok, _records} =
             ReleaseController.retrieve(controller, ctx.target, %{limit: 10},
               actor_id: ctx.target.id
             )

    assert {:ok, _records} =
             ReleaseController.retrieve(controller, ctx.target, %{limit: 10},
               actor_id: ctx.target.id
             )

    assert {:ok, true} = ReleaseController.cutover_ready?(controller)
    assert {:ok, :memory_os} = ReleaseController.set_mode(controller, :memory_os)

    assert {:ok, :memory_os} = ReleaseController.mode(controller)

    assert {:ok, _record} =
             ReleaseController.remember(
               controller,
               ctx.target,
               %{id: "phase8-post-cutover", class: :episodic, kind: :event, text: "post cutover"},
               actor_id: ctx.target.id
             )

    assert {:ok, []} =
             Runtime.recall(ctx.target, %{
               namespace: legacy_namespace(ctx.target),
               store: ctx.legacy_store,
               text_contains: "post cutover",
               limit: 10
             })

    assert {:ok, memory_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "post cutover", limit: 10},
               server: ctx.manager,
               actor_id: ctx.target.id
             )

    assert Enum.any?(memory_records, &(&1.id == "phase8-post-cutover"))
  end

  test "rollback trigger switches mode to legacy on SLO breach", ctx do
    controller =
      start_release_controller!(ctx,
        mode: :memory_os,
        rollout_stage: :global,
        rollback_thresholds: %{latency_p95_ms: 120, error_rate: 0.02, queue_depth: 40}
      )

    assert {:ok, :stable} =
             ReleaseController.evaluate_rollback(controller, %{
               latency_p95_ms: 100,
               error_rate: 0.01,
               queue_depth: 20
             })

    assert {:ok, {:rollback, reason}} =
             ReleaseController.evaluate_rollback(controller, %{
               latency_p95_ms: 400,
               error_rate: 0.01,
               queue_depth: 20
             })

    assert reason.mode_before == :memory_os
    assert reason.mode_after == :legacy

    assert {:ok, :legacy} = ReleaseController.mode(controller)

    assert {:ok, status} = ReleaseController.status(controller)
    assert status.metrics.rollbacks == 1
    assert is_map(status.last_rollback)
  end

  test "fallback retrieval returns legacy records when MemoryOS path fails", ctx do
    controller =
      start_release_controller!(ctx,
        mode: :memory_os,
        rollout_stage: :global,
        fallback_enabled: true,
        memory_server: :missing_phase8_memory_manager
      )

    seed_legacy(ctx, %{id: "phase8-fallback", class: :episodic, text: "fallback text"})

    assert {:ok, records} =
             ReleaseController.retrieve(
               controller,
               ctx.target,
               %{text_contains: "fallback text", limit: 10},
               actor_id: ctx.target.id
             )

    assert Enum.any?(records, &(&1.id == "phase8-fallback"))

    assert {:ok, status} = ReleaseController.status(controller)
    assert status.metrics.fallbacks >= 1
  end

  test "post-rollback reconciliation repairs drift from legacy source of truth", ctx do
    controller = start_release_controller!(ctx, mode: :legacy, rollout_stage: :global)

    seed_legacy(ctx, %{id: "phase8-reconcile-a", class: :episodic, text: "reconcile a"})
    seed_legacy(ctx, %{id: "phase8-reconcile-b", class: :semantic, text: "reconcile b"})

    assert {:ok, before} =
             Migration.reconcile_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id
             )

    assert before.legacy_count == 2
    assert before.memory_os_count == 0
    refute before.parity?

    assert {:ok, summary} =
             ReleaseController.reconcile_post_rollback(controller, [ctx.target],
               actor_id: ctx.target.id,
               on_error: :halt
             )

    assert summary.targets == 1
    assert summary.successful == 1
    assert summary.migrated_count == 2

    assert {:ok, after_report} =
             Migration.reconcile_target(ctx.target,
               legacy_store: ctx.legacy_store,
               server: ctx.manager,
               actor_id: ctx.target.id
             )

    assert after_report.parity?
    assert after_report.memory_os_count == 2
  end

  defp start_release_controller!(ctx, opts) do
    name =
      :"phase8-release-controller-#{ctx.unique}-#{System.unique_integer([:positive, :monotonic])}"

    start_supervised!(
      {ReleaseController,
       [
         name: name,
         memory_server: Keyword.get(opts, :memory_server, ctx.manager),
         legacy_store: ctx.legacy_store
       ] ++ Keyword.drop(opts, [:memory_server])},
      id: name
    )

    name
  end

  defp retrieve_tier(ctx, tier, text) do
    Jido.MemoryOS.retrieve(
      ctx.target,
      %{text_contains: text, limit: 10},
      server: ctx.manager,
      actor_id: ctx.target.id,
      tier: tier,
      tier_mode: tier
    )
  end

  defp seed_legacy(ctx, attrs) do
    now = System.system_time(:millisecond)

    defaults = %{
      class: :episodic,
      kind: :event,
      text: "legacy",
      observed_at: now,
      metadata: %{}
    }

    payload = Map.merge(defaults, attrs)

    assert {:ok, _record} =
             Runtime.remember(ctx.target, payload,
               namespace: legacy_namespace(ctx.target),
               store: ctx.legacy_store
             )
  end

  defp legacy_namespace(target), do: "agent:" <> target.id

  defp phase8_app_config(tables) do
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
        masking: %{default_mode: :allow, role_modes: %{admin: :allow}}
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
