defmodule Jido.MemoryOS.Phase02IntegrationTest do
  use ExUnit.Case, async: false

  alias Jido.MemoryOS.{MemoryManager, Metadata}

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    manager = :"phase2-memory-manager-#{unique}"
    target = %{id: "phase2-agent-#{unique}"}

    tables = %{
      short: :"jido_memory_os_phase2_#{unique}_short",
      mid: :"jido_memory_os_phase2_#{unique}_mid",
      long: :"jido_memory_os_phase2_#{unique}_long"
    }

    cleanup_tables(tables)

    start_supervised!({MemoryManager, name: manager, app_config: phase2_app_config(tables)})

    on_exit(fn -> cleanup_tables(tables) end)

    {:ok, manager: manager, target: target, now: System.system_time(:millisecond)}
  end

  test "consolidation promotes short events into mid/page and long records with lineage", ctx do
    opts = manager_opts(ctx)

    source_ids =
      for index <- 1..3 do
        {:ok, record} =
          Jido.MemoryOS.remember(
            ctx.target,
            %{
              class: :episodic,
              kind: :event,
              text: "phase2 event #{index} for deployment history",
              content: %{index: index, topic: "deployment"},
              tags: ["persona:ops", "topic:deploy"],
              chain_id: "chain:phase2:lineage",
              observed_at: ctx.now + index
            },
            opts
          )

        record.id
      end

    assert {:ok, summary} = Jido.MemoryOS.consolidate(ctx.target, opts)
    assert summary.short_candidates_processed == 3
    assert summary.mid_segments_written == 2
    assert summary.mid_pages_written == 2
    assert summary.long_promoted == 2

    assert {:ok, segments} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{kinds: [:segment], limit: 20},
               with_tier(opts, :mid)
             )

    assert length(segments) == 2

    assert {:ok, pages} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{kinds: [:page], limit: 20},
               with_tier(opts, :mid)
             )

    assert length(pages) == 2

    assert {:ok, long_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["memory_os:long"], limit: 20},
               with_tier(opts, :long)
             )

    assert length(long_records) == 2
    assert {:ok, mem_os} = Metadata.from_record(hd(long_records))
    assert mem_os.tier == :long

    provenance =
      long_records
      |> hd()
      |> map_get(:content, %{})
      |> map_get(:provenance, %{})

    promoted_source_ids = map_get(provenance, :source_short_ids, [])

    assert is_list(promoted_source_ids)
    assert promoted_source_ids != []
    assert Enum.all?(promoted_source_ids, &(&1 in source_ids))
  end

  test "short tier rolling cap evicts oldest records", ctx do
    opts =
      manager_opts(ctx)
      |> Keyword.put(:tiers, %{short: %{max_records: 3}})

    inserted_ids =
      for index <- 1..5 do
        {:ok, record} =
          Jido.MemoryOS.remember(
            ctx.target,
            %{
              class: :episodic,
              kind: :event,
              text: "rolling short #{index}",
              tags: ["topic:rolling"],
              chain_id: "chain:phase2:rolling",
              observed_at: ctx.now + index
            },
            opts
          )

        record.id
      end

    assert {:ok, short_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{limit: 20, order: :asc},
               with_tier(opts, :short)
             )

    remaining_ids = Enum.map(short_records, & &1.id)

    assert length(short_records) == 3
    assert Enum.sort(remaining_ids) == Enum.sort(Enum.take(inserted_ids, -3))
    refute Enum.any?(remaining_ids, &(&1 in Enum.take(inserted_ids, 2)))
  end

  test "invalid lifecycle transitions are rejected", ctx do
    opts =
      manager_opts(ctx)
      |> Keyword.put(:tier, :long)
      |> Keyword.put(:previous_tier, :short)

    assert {:error, %Jido.Error.ValidationError{} = error} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{class: :semantic, kind: :fact, text: "should not bypass mid tier"},
               opts
             )

    assert error.kind == :input
    assert error.subject == :tier
    assert get_in(error.details, [:code]) == :invalid_lifecycle_transition
  end

  test "duplicate short events are idempotent and consolidate once", ctx do
    opts = manager_opts(ctx)

    attrs = %{
      class: :episodic,
      kind: :event,
      text: "duplicate short event",
      content: %{topic: "idempotency"},
      tags: ["persona:ops", "topic:idempotency"],
      chain_id: "chain:phase2:dupe"
    }

    assert {:ok, first} = Jido.MemoryOS.remember(ctx.target, attrs, opts)
    assert {:ok, second} = Jido.MemoryOS.remember(ctx.target, attrs, opts)
    assert first.id == second.id

    assert {:ok, short_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{text_contains: "duplicate short event", limit: 20},
               with_tier(opts, :short)
             )

    assert Enum.count(short_records, &(&1.id == first.id)) == 1

    assert {:ok, summary} = Jido.MemoryOS.consolidate(ctx.target, opts)
    assert summary.short_candidates_processed == 1
  end

  test "concurrent consolidation requests keep lifecycle state consistent", ctx do
    opts = manager_opts(ctx)

    for index <- 1..6 do
      assert {:ok, _record} =
               Jido.MemoryOS.remember(
                 ctx.target,
                 %{
                   class: :episodic,
                   kind: :event,
                   text: "concurrency stream #{index}",
                   content: %{index: index},
                   tags: ["persona:ops", "topic:concurrency"],
                   chain_id: "chain:phase2:concurrency",
                   observed_at: ctx.now + index
                 },
                 opts
               )
    end

    results =
      1..4
      |> Task.async_stream(
        fn _ -> Jido.MemoryOS.consolidate(ctx.target, opts) end,
        ordered: false,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, fn
             {:ok, %{status: :ok}} -> true
             _ -> false
           end)

    assert {:ok, long_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["memory_os:long"], limit: 50},
               with_tier(opts, :long)
             )

    long_ids = Enum.map(long_records, & &1.id)

    assert long_records != []
    assert long_ids == Enum.uniq(long_ids)
  end

  test "version conflict strategy keeps history and exposes conflict reasons", ctx do
    opts = manager_opts(ctx)

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user prefers concise summaries",
                 tags: ["persona:writer", "fact_key:user.preference.summary_style"],
                 chain_id: "chain:phase2:conflict",
                 observed_at: ctx.now + 1
               },
               opts
             )

    assert {:ok, _summary} = Jido.MemoryOS.consolidate(ctx.target, opts)

    assert {:ok, _record} =
             Jido.MemoryOS.remember(
               ctx.target,
               %{
                 class: :episodic,
                 kind: :event,
                 text: "user now prefers detailed summaries",
                 tags: ["persona:writer", "fact_key:user.preference.summary_style"],
                 chain_id: "chain:phase2:conflict",
                 observed_at: ctx.now + 2
               },
               opts
             )

    assert {:ok, summary} = Jido.MemoryOS.consolidate(ctx.target, opts)
    assert summary.conflicts != []

    assert {:ok, recent_conflicts} = MemoryManager.last_conflicts(ctx.manager)

    assert Enum.any?(recent_conflicts, fn conflict ->
             conflict.fact_key == "user.preference.summary_style" and
               conflict.strategy == :version
           end)

    assert {:ok, explain} =
             Jido.MemoryOS.explain_retrieval(
               ctx.target,
               %{tags_any: ["fact_key:user.preference.summary_style"], limit: 20},
               with_tier(opts, :long)
             )

    assert explain.recent_conflicts != []

    assert {:ok, long_records} =
             Jido.MemoryOS.retrieve(
               ctx.target,
               %{tags_any: ["fact_key:user.preference.summary_style"], limit: 20},
               with_tier(opts, :long)
             )

    assert length(long_records) >= 2

    assert Enum.any?(long_records, fn record ->
             conflict = map_get(record.metadata || %{}, :mem_os_conflict, %{})
             map_get(conflict, :reason) == "superseded"
           end)
  end

  defp manager_opts(ctx), do: [server: ctx.manager]

  defp with_tier(opts, tier), do: Keyword.put(opts, :tier, tier)

  defp phase2_app_config(tables) do
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

  defp map_get(map, key, default \\ nil) when is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end
end
