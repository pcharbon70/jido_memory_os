defmodule Jido.MemoryOS.Phase06IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.{DataSafety, MemoryManager}

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase6-memory-manager-#{unique}"
    target = %{id: "phase6-agent-#{unique}", group: "group-alpha"}

    tables = %{
      short: :"jido_memory_os_phase6_#{unique}_short",
      mid: :"jido_memory_os_phase6_#{unique}_mid",
      long: :"jido_memory_os_phase6_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: phase6_app_config(tables)},
      id: manager
    )

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok, manager: manager, target: target, tables: tables}
  end

  test "same-group actor is allowed by policy defaults", ctx do
    opts =
      manager_opts(ctx,
        actor_id: "actor-1",
        actor_group: "group-alpha",
        target_group: "group-alpha"
      )

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{text: "same group write allowed", class: :episodic, kind: :event},
               opts
             )

    assert {:ok, records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "same group write allowed", limit: 5},
               opts
             )

    assert length(records) >= 1
  end

  test "cross-group actor is denied with structured access error", ctx do
    opts =
      manager_opts(ctx,
        actor_id: "actor-cross",
        actor_group: "group-beta"
      )

    assert {:error, %Jido.Error.ExecutionError{} = error} =
             Jido.MemoryOS.retrieve(ctx.target, %{limit: 1}, opts)

    assert get_in(error.details, [:code]) == :access_denied
    assert get_in(error.details, [:policy_reason]) == :cross_agent_denied
  end

  test "approval-gated forget flow requires valid token and audits decisions", ctx do
    opts = manager_opts(ctx, actor_id: ctx.target.id)

    assert {:ok, record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{text: "approval gated deletion", class: :episodic, kind: :event},
               opts
             )

    assert {:error, %Jido.Error.ExecutionError{} = denied} =
             Jido.MemoryOS.forget(ctx.target, record.id, opts)

    assert get_in(denied.details, [:code]) == :approval_required

    assert {:ok, token_entry} =
             MemoryManager.issue_approval_token(
               ctx.manager,
               actor_id: ctx.target.id,
               actions: [:forget],
               reason: "test approval"
             )

    assert is_binary(token_entry.token)

    assert {:ok, true} =
             Jido.MemoryOS.forget(
               ctx.target,
               record.id,
               Keyword.put(opts, :approval_token, token_entry.token)
             )

    assert {:error, %Jido.Error.ExecutionError{} = invalid} =
             Jido.MemoryOS.forget(
               ctx.target,
               record.id,
               Keyword.put(opts, :approval_token, token_entry.token)
             )

    assert get_in(invalid.details, [:code]) == :approval_invalid

    assert {:ok, audits} = MemoryManager.audit_events(ctx.manager, limit: 50)
    assert Enum.any?(audits, &(&1.category == :approval and &1.outcome == :issued))
    assert Enum.any?(audits, &(&1.category == :approval and &1.outcome == :denied))
    assert Enum.any?(audits, &(&1.category == :approval and &1.outcome == :approved))
  end

  test "retention policy blocks forbidden tags before persistence", ctx do
    opts = manager_opts(ctx, actor_id: ctx.target.id)

    assert {:error, %Jido.Error.ValidationError{} = error} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 text: "blocked by retention",
                 class: :episodic,
                 kind: :event,
                 tags: ["restricted"]
               },
               opts
             )

    assert get_in(error.details, [:code]) == :retention_blocked
    assert get_in(error.details, [:reason_kind]) == :tag
    assert get_in(error.details, [:value]) == "restricted"
  end

  test "pre-persist redaction masks pii before storage", _ctx do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase6-redact-manager-#{unique}"
    target = %{id: "phase6-redact-agent-#{unique}", group: "group-alpha"}

    tables = %{
      short: :"jido_memory_os_phase6_redact_#{unique}_short",
      mid: :"jido_memory_os_phase6_redact_#{unique}_mid",
      long: :"jido_memory_os_phase6_redact_#{unique}_long"
    }

    cleanup_tables(tables)

    app_config =
      phase6_app_config(tables)
      |> put_in([:safety, :redaction_enabled], true)
      |> put_in([:safety, :pii_strategy], :mask)

    start_supervised!({MemoryManager, name: manager, app_config: app_config}, id: manager)
    on_exit(fn -> cleanup_tables(tables) end)

    opts = [server: manager, actor_id: target.id]

    raw_text = "email me at person@example.com or call 415-555-1212"

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               target,
               %{text: raw_text, class: :episodic, kind: :event},
               opts
             )

    assert {:ok, [stored | _]} =
             Jido.MemoryOS.retrieve(target, %{text_contains: "email me", limit: 1}, opts)

    assert stored.text =~ "[REDACTED_EMAIL]"
    assert stored.text =~ "[REDACTED_PHONE]"
    refute stored.text =~ "person@example.com"
    refute stored.text =~ "415-555-1212"
  end

  test "retrieval masking follows role-based governance policy", ctx do
    raw_text = "contact: viewer@example.com / 212-555-0191"

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{text: raw_text, class: :episodic, kind: :event},
               manager_opts(ctx, actor_id: ctx.target.id)
             )

    assert {:ok, [masked | _]} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "contact:", limit: 1},
               manager_opts(ctx, actor_id: ctx.target.id, actor_role: "reviewer")
             )

    assert masked.text =~ "[REDACTED_EMAIL]"
    assert masked.text =~ "[REDACTED_PHONE]"

    assert {:ok, [admin | _]} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "contact:", limit: 1},
               manager_opts(ctx, actor_id: ctx.target.id, actor_role: "admin")
             )

    assert admin.text =~ "viewer@example.com"
    assert admin.text =~ "212-555-0191"
  end

  test "audit log captures access decisions, mutation pointers, and explain hash", ctx do
    opts = manager_opts(ctx, actor_id: ctx.target.id, correlation_id: "phase6-audit")
    explicit_id = "phase6-audit-id-#{System.unique_integer([:positive])}"

    assert {:ok, overwrite_token} =
             MemoryManager.issue_approval_token(
               ctx.manager,
               actor_id: ctx.target.id,
               actions: [:overwrite]
             )

    assert {:ok, %Jido.Memory.Record{id: ^explicit_id}} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{id: explicit_id, text: "audit trace payload", class: :episodic, kind: :event},
               Keyword.put(opts, :approval_token, overwrite_token.token)
             )

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{text_contains: "audit trace payload", limit: 1},
               opts
             )

    assert {:ok, events} = MemoryManager.audit_events(ctx.manager, limit: 100)

    remember_access =
      Enum.find(events, fn event ->
        event.category == :access and event.action == :remember and event.outcome == :allow
      end)

    assert is_map(remember_access)
    assert remember_access.actor_id == ctx.target.id

    remember_operation =
      Enum.find(events, fn event ->
        event.category == :operation and event.action == :remember and event.outcome == :ok
      end)

    assert is_map(remember_operation)
    assert get_in(remember_operation, [:metadata, :before_pointer, :memory_id]) == explicit_id
    assert get_in(remember_operation, [:metadata, :after_pointer, :memory_id]) == explicit_id

    explain_operation =
      Enum.find(events, fn event ->
        event.category == :operation and event.action == :explain_retrieval and
          event.outcome == :ok
      end)

    assert is_map(explain_operation)

    expected_hash = DataSafety.explanation_hash(explain)

    assert get_in(explain_operation, [:metadata, :after_pointer, :explanation_hash]) ==
             expected_hash

    assert byte_size(expected_hash) == 64
  end

  defp manager_opts(ctx, extra) do
    [server: ctx.manager] ++ extra
  end

  defp phase6_app_config(tables) do
    %{
      tiers: %{
        short: %{
          store: {Jido.Memory.Store.ETS, [table: tables.short]},
          max_records: 500,
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
      safety: %{
        audit_enabled: true,
        redaction_enabled: false,
        pii_strategy: :mask
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
      },
      governance: %{
        policy: %{
          default_effect: :deny,
          allow_same_group: true,
          rules: []
        },
        approvals: %{
          enabled: true,
          required_actions: [:forget, :policy_update, :overwrite],
          ttl_ms: 60_000,
          max_tokens: 100,
          one_time: true
        },
        audit: %{
          enabled: true,
          max_events: 500
        },
        retention: %{
          short: %{max_ttl_ms: nil, blocked_tags: ["restricted"], allowed_classes: []},
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
      for suffix <- ["_records", "_meta"] do
        derived = :"#{table}#{suffix}"
        if :ets.whereis(derived) != :undefined, do: :ets.delete(derived)
      end

      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)
  end
end
