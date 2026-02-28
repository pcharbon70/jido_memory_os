defmodule Jido.MemoryOS.MemoryManager do
  @moduledoc """
  Phase 2 orchestration process for tiered memory lifecycle.
  """

  use GenServer

  alias Jido.MemoryOS.Adapter.MemoryRuntime
  alias Jido.MemoryOS.{Config, ErrorMapping, Lifecycle, Metadata}

  @type candidate :: {String.t(), String.t()}

  @type state :: %{
          app_config: map(),
          short_candidates: MapSet.t(candidate()),
          long_candidates: MapSet.t(candidate()),
          turn_counters: %{optional(String.t()) => non_neg_integer()},
          consolidation_version: pos_integer(),
          last_conflicts: [map()]
        }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Writes one memory record.
  """
  @spec remember(map() | struct(), map() | keyword(), keyword()) ::
          {:ok, Jido.Memory.Record.t()} | {:error, term()}
  def remember(target, attrs, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:remember, target, attrs, runtime_opts})
  end

  @doc """
  Retrieves records by query.
  """
  @spec retrieve(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  def retrieve(target, query, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:retrieve, target, query, runtime_opts})
  end

  @doc """
  Deletes one record by id.
  """
  @spec forget(map() | struct(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def forget(target, id, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:forget, target, id, runtime_opts})
  end

  @doc """
  Prunes expired records from the selected store.
  """
  @spec prune(map() | struct(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune(target, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:prune, target, runtime_opts})
  end

  @doc """
  Runs lifecycle consolidation for short->mid->long tier transitions.
  """
  @spec consolidate(map() | struct(), keyword()) :: {:ok, map()} | {:error, term()}
  def consolidate(target, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:consolidate, target, runtime_opts}, :infinity)
  end

  @doc """
  Returns retrieval diagnostics payload.
  """
  @spec explain_retrieval(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def explain_retrieval(target, query, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)

    with {:ok, records} <- retrieve(target, query, opts),
         {:ok, config} <- current_config(server),
         {:ok, conflicts} <- last_conflicts(server) do
      {:ok,
       %{
         query: query,
         result_count: length(records),
         retrieval: config.retrieval,
         tier: Keyword.get(opts, :tier, :short),
         lifecycle: config.lifecycle,
         records: Enum.map(records, &record_debug/1),
         recent_conflicts: conflicts
       }}
    end
  end

  @doc """
  Returns currently loaded app config after validation/defaulting.
  """
  @spec current_config(GenServer.server()) :: {:ok, Config.t()} | {:error, term()}
  def current_config(server \\ __MODULE__) do
    GenServer.call(server, :current_config)
  end

  @doc """
  Returns recent lifecycle conflict events.
  """
  @spec last_conflicts(GenServer.server()) :: {:ok, [map()]}
  def last_conflicts(server \\ __MODULE__) do
    GenServer.call(server, :last_conflicts)
  end

  @impl true
  def init(opts) do
    app_config = Keyword.get(opts, :app_config, Config.app_config())

    {:ok,
     %{
       app_config: app_config,
       short_candidates: MapSet.new(),
       long_candidates: MapSet.new(),
       turn_counters: %{},
       consolidation_version: 1,
       last_conflicts: []
     }}
  end

  @impl true
  def handle_call({:remember, target, attrs, runtime_opts}, _from, state) do
    runtime_opts = with_app_config(runtime_opts, state)

    case normalize_tier(Keyword.get(runtime_opts, :tier, :short)) do
      {:ok, :short} ->
        {result, new_state} = remember_short(target, attrs, runtime_opts, state)
        {:reply, result, new_state}

      {:ok, _tier} ->
        {:reply, MemoryRuntime.remember(target, attrs, runtime_opts), state}

      {:error, reason} ->
        {:reply, {:error, ErrorMapping.from_reason(reason, :remember)}, state}
    end
  end

  @impl true
  def handle_call({:retrieve, target, query, runtime_opts}, _from, state) do
    result = MemoryRuntime.recall(target, query, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:forget, target, id, runtime_opts}, _from, state) do
    result = MemoryRuntime.forget(target, id, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:prune, target, runtime_opts}, _from, state) do
    result = MemoryRuntime.prune(target, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:consolidate, target, runtime_opts}, _from, state) do
    runtime_opts = with_app_config(runtime_opts, state)
    {result, new_state} = run_consolidation(target, runtime_opts, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call(:current_config, _from, state) do
    {:reply, Config.validate(state.app_config), state}
  end

  @impl true
  def handle_call(:last_conflicts, _from, state) do
    {:reply, {:ok, state.last_conflicts}, state}
  end

  @spec remember_short(map() | struct(), map() | keyword(), keyword(), state()) ::
          {{:ok, Jido.Memory.Record.t()} | {:error, term()}, state()}
  defp remember_short(target, attrs, runtime_opts, state) do
    now = System.system_time(:millisecond)

    with {:ok, context} <- MemoryRuntime.resolve_context(target, runtime_opts) do
      turn_index = Map.get(state.turn_counters, context.namespace, 0)
      normalized = Lifecycle.normalize_short_event(attrs, context.namespace, turn_index, now)

      attrs_with_ingest = merge_ingest_metadata(normalized.attrs, normalized.ingest_meta)
      mem_os_override = normalize_map(Keyword.get(runtime_opts, :mem_os, %{}))
      mem_os_updates = Map.merge(normalized.mem_os, mem_os_override)

      remember_opts =
        runtime_opts
        |> Keyword.put(:tier, :short)
        |> Keyword.put(:mem_os, mem_os_updates)

      case MemoryRuntime.remember(target, attrs_with_ingest, remember_opts) do
        {:ok, record} ->
          updated_state =
            state
            |> put_turn_counter(context.namespace, normalized.next_turn_index)
            |> enqueue_short_candidate(context.namespace, record.id)
            |> enforce_short_maintenance(target, runtime_opts, context)

          {{:ok, record}, updated_state}

        {:error, reason} ->
          {{:error, to_jido_error(reason, :remember)}, state}
      end
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, :remember)}, state}
    end
  end

  @spec run_consolidation(map() | struct(), keyword(), state()) ::
          {{:ok, map()} | {:error, term()}, state()}
  defp run_consolidation(target, runtime_opts, state) do
    with {:ok, short_context} <-
           MemoryRuntime.resolve_context(target, Keyword.put(runtime_opts, :tier, :short)),
         {:ok, mid_context} <-
           MemoryRuntime.resolve_context(target, Keyword.put(runtime_opts, :tier, :mid)),
         {:ok, long_context} <-
           MemoryRuntime.resolve_context(target, Keyword.put(runtime_opts, :tier, :long)),
         {:ok, short_records, state1} <-
           consume_short_candidates(target, runtime_opts, short_context, state),
         {:ok, segment_records, page_records, state2} <-
           write_mid_records(target, runtime_opts, short_records, mid_context, state1),
         {:ok, state3} <- enqueue_long_candidates(mid_context.namespace, page_records, state2),
         {:ok, promoted_records, conflicts, state4} <-
           promote_long_records(target, runtime_opts, mid_context, long_context, state3) do
      summary = %{
        status: :ok,
        phase: 2,
        short_candidates_processed: length(short_records),
        mid_segments_written: Enum.count(segment_records, &(&1.kind == :segment)),
        mid_pages_written: Enum.count(page_records),
        long_promoted: length(promoted_records),
        conflicts: conflicts,
        consolidation_version: state4.consolidation_version
      }

      {{:ok, summary}, %{state4 | last_conflicts: conflicts}}
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, :consolidate)}, state}
    end
  end

  @spec consume_short_candidates(map() | struct(), keyword(), map(), state()) ::
          {:ok, [Jido.Memory.Record.t()], state()} | {:error, term()}
  defp consume_short_candidates(target, runtime_opts, short_context, state) do
    {selected, remaining} = split_candidates(state.short_candidates, short_context.namespace)

    selected
    |> Enum.reduce_while(
      {:ok, [], %{state | short_candidates: MapSet.new(remaining)}},
      fn {_namespace, id}, {:ok, acc, acc_state} ->
        case MemoryRuntime.get(target, id, Keyword.put(runtime_opts, :tier, :short)) do
          {:ok, record} -> {:cont, {:ok, [record | acc], acc_state}}
          {:error, %Jido.Error.ExecutionError{}} -> {:cont, {:ok, acc, acc_state}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
    |> case do
      {:ok, records, new_state} -> {:ok, Enum.reverse(records), new_state}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec write_mid_records(map() | struct(), keyword(), [Jido.Memory.Record.t()], map(), state()) ::
          {:ok, [Jido.Memory.Record.t()], [Jido.Memory.Record.t()], state()} | {:error, term()}
  defp write_mid_records(_target, _runtime_opts, [], _mid_context, state),
    do: {:ok, [], [], state}

  defp write_mid_records(target, runtime_opts, short_records, mid_context, state) do
    lifecycle = mid_context.config.lifecycle
    grouped = Lifecycle.group_by_chain(short_records, "chain:" <> mid_context.namespace)
    now = System.system_time(:millisecond)
    mid_ttl = mid_context.config.tiers.mid.ttl_ms

    with {:ok, segment_records} <-
           write_segments(
             target,
             runtime_opts,
             grouped,
             lifecycle,
             mid_ttl,
             state.consolidation_version,
             now
           ),
         {:ok, page_records} <-
           write_pages(
             target,
             runtime_opts,
             Lifecycle.group_by_chain(segment_records, "chain:" <> mid_context.namespace),
             lifecycle,
             mid_ttl,
             state.consolidation_version,
             now
           ) do
      {:ok, segment_records, page_records, state}
    end
  end

  @spec write_segments(
          map() | struct(),
          keyword(),
          %{String.t() => [Jido.Memory.Record.t()]},
          map(),
          pos_integer(),
          pos_integer(),
          integer()
        ) :: {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  defp write_segments(
         target,
         runtime_opts,
         grouped,
         lifecycle,
         mid_ttl,
         consolidation_version,
         now
       ) do
    grouped
    |> Enum.flat_map(fn {chain_id, records} ->
      records
      |> Lifecycle.segment_records(lifecycle.segment_max_events, lifecycle.segment_max_tokens)
      |> Enum.map(&{chain_id, &1})
    end)
    |> Enum.reduce_while({:ok, []}, fn {chain_id, segment_source_records}, {:ok, acc} ->
      segment =
        Lifecycle.build_mid_segment(
          segment_source_records,
          chain_id,
          mid_ttl,
          consolidation_version,
          now
        )

      opts =
        runtime_opts
        |> Keyword.put(:tier, :mid)
        |> Keyword.put(:mem_os, segment.mem_os)
        |> Keyword.put(:previous_tier, :short)

      case MemoryRuntime.remember(target, segment.attrs, opts) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec write_pages(
          map() | struct(),
          keyword(),
          %{String.t() => [Jido.Memory.Record.t()]},
          map(),
          pos_integer(),
          pos_integer(),
          integer()
        ) :: {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  defp write_pages(target, runtime_opts, grouped, lifecycle, mid_ttl, consolidation_version, now) do
    grouped
    |> Enum.flat_map(fn {chain_id, segment_records} ->
      segment_records
      |> Enum.chunk_every(lifecycle.page_max_segments)
      |> Enum.map(&{chain_id, &1})
    end)
    |> Enum.reduce_while({:ok, []}, fn {chain_id, page_segment_records}, {:ok, acc} ->
      page =
        Lifecycle.build_mid_page(
          page_segment_records,
          chain_id,
          mid_ttl,
          consolidation_version,
          now
        )

      opts =
        runtime_opts
        |> Keyword.put(:tier, :mid)
        |> Keyword.put(:mem_os, page.mem_os)
        |> Keyword.put(:previous_tier, :short)

      case MemoryRuntime.remember(target, page.attrs, opts) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec enqueue_long_candidates(String.t(), [Jido.Memory.Record.t()], state()) :: {:ok, state()}
  defp enqueue_long_candidates(namespace, page_records, state) do
    candidates =
      Enum.reduce(page_records, state.long_candidates, fn page_record, acc ->
        MapSet.put(acc, {namespace, page_record.id})
      end)

    {:ok, %{state | long_candidates: candidates}}
  end

  @spec promote_long_records(map() | struct(), keyword(), map(), map(), state()) ::
          {:ok, [Jido.Memory.Record.t()], [map()], state()} | {:error, term()}
  defp promote_long_records(target, runtime_opts, mid_context, long_context, state) do
    {selected, remaining} = split_candidates(state.long_candidates, mid_context.namespace)
    long_ttl = long_context.config.tiers.long.ttl_ms
    now = System.system_time(:millisecond)

    with {:ok, pages} <- load_pages(target, runtime_opts, selected),
         {:ok, promoted, conflicts, requeue, updated_state} <-
           promote_page_records(target, runtime_opts, pages, long_context, long_ttl, now, state) do
      new_long_candidates =
        requeue
        |> Enum.reduce(MapSet.new(remaining), fn candidate, acc -> MapSet.put(acc, candidate) end)

      {:ok, promoted, conflicts, %{updated_state | long_candidates: new_long_candidates}}
    end
  end

  @spec load_pages(map() | struct(), keyword(), [candidate()]) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  defp load_pages(target, runtime_opts, selected_candidates) do
    selected_candidates
    |> Enum.reduce_while({:ok, []}, fn {_namespace, id}, {:ok, acc} ->
      case MemoryRuntime.get(target, id, Keyword.put(runtime_opts, :tier, :mid)) do
        {:ok, page} -> {:cont, {:ok, [page | acc]}}
        {:error, %Jido.Error.ExecutionError{}} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, pages} -> {:ok, Enum.reverse(pages)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec promote_page_records(
          map() | struct(),
          keyword(),
          [Jido.Memory.Record.t()],
          map(),
          pos_integer(),
          integer(),
          state()
        ) :: {:ok, [Jido.Memory.Record.t()], [map()], [candidate()], state()} | {:error, term()}
  defp promote_page_records(target, runtime_opts, pages, long_context, long_ttl, now, state) do
    recurrence = Enum.frequencies_by(pages, &Lifecycle.fact_key/1)

    min_score =
      max(
        long_context.config.tiers.long.promotion_threshold,
        long_context.config.lifecycle.promotion_min_score
      )

    Enum.reduce_while(
      pages,
      {:ok, [], [], [], state},
      fn page, {:ok, promoted, conflicts, requeue, acc_state} ->
        fact_key = Lifecycle.fact_key(page)
        score = Lifecycle.promotion_score(page, Map.get(recurrence, fact_key, 1), min_score)

        if score.eligible? do
          case promote_page(
                 target,
                 runtime_opts,
                 page,
                 fact_key,
                 score.score,
                 long_context,
                 long_ttl,
                 now,
                 acc_state
               ) do
            {:ok, promoted_record, conflict_entry, new_state} ->
              next_conflicts =
                if is_map(conflict_entry), do: [conflict_entry | conflicts], else: conflicts

              {:cont, {:ok, [promoted_record | promoted], next_conflicts, requeue, new_state}}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        else
          {:cont, {:ok, promoted, conflicts, [{page.namespace, page.id} | requeue], acc_state}}
        end
      end
    )
    |> case do
      {:ok, promoted, conflicts, requeue, new_state} ->
        {:ok, Enum.reverse(promoted), Enum.reverse(conflicts), requeue, new_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec promote_page(
          map() | struct(),
          keyword(),
          Jido.Memory.Record.t(),
          String.t(),
          number(),
          map(),
          pos_integer(),
          integer(),
          state()
        ) :: {:ok, Jido.Memory.Record.t(), map() | nil, state()} | {:error, term()}
  defp promote_page(
         target,
         runtime_opts,
         page,
         fact_key,
         score,
         long_context,
         long_ttl,
         now,
         state
       ) do
    strategy = long_context.config.lifecycle.conflict_strategy

    with {:ok, existing} <-
           MemoryRuntime.recall(
             target,
             %{tags_any: ["fact_key:" <> fact_key], limit: 100, order: :desc},
             Keyword.put(runtime_opts, :tier, :long)
           ),
         :ok <- maybe_apply_conflict_strategy(target, runtime_opts, existing, strategy),
         {consolidation_version, state} <- next_consolidation_version(state),
         long_spec <-
           Lifecycle.build_long_record(
             page,
             fact_key,
             score,
             long_ttl,
             consolidation_version,
             now,
             strategy,
             Enum.map(existing, & &1.id)
           ),
         {:ok, promoted_record} <-
           MemoryRuntime.remember(
             target,
             long_spec.attrs,
             runtime_opts
             |> Keyword.put(:tier, :long)
             |> Keyword.put(:mem_os, long_spec.mem_os)
             |> Keyword.put(:previous_tier, :mid)
             |> Keyword.put(:strict_transition, false)
           ),
         :ok <-
           mark_superseded_records(
             target,
             runtime_opts,
             existing,
             promoted_record.id,
             fact_key,
             consolidation_version,
             now,
             strategy
           ) do
      conflict_entry =
        if existing == [] do
          nil
        else
          %{
            fact_key: fact_key,
            strategy: strategy,
            previous_ids: Enum.map(existing, & &1.id),
            promoted_id: promoted_record.id
          }
        end

      {:ok, promoted_record, conflict_entry, state}
    end
  end

  @spec maybe_apply_conflict_strategy(
          map() | struct(),
          keyword(),
          [Jido.Memory.Record.t()],
          atom()
        ) :: :ok | {:error, term()}
  defp maybe_apply_conflict_strategy(_target, _runtime_opts, [], _strategy), do: :ok

  defp maybe_apply_conflict_strategy(target, runtime_opts, existing, :replace) do
    existing
    |> Enum.reduce_while(:ok, fn record, :ok ->
      case MemoryRuntime.forget(target, record.id, Keyword.put(runtime_opts, :tier, :long)) do
        {:ok, _deleted?} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp maybe_apply_conflict_strategy(_target, _runtime_opts, _existing, :append), do: :ok
  defp maybe_apply_conflict_strategy(_target, _runtime_opts, _existing, :version), do: :ok

  @spec mark_superseded_records(
          map() | struct(),
          keyword(),
          [Jido.Memory.Record.t()],
          String.t(),
          String.t(),
          pos_integer(),
          integer(),
          atom()
        ) :: :ok | {:error, term()}
  defp mark_superseded_records(
         _target,
         _runtime_opts,
         _existing,
         _new_id,
         _fact_key,
         _version,
         _now,
         strategy
       )
       when strategy != :version,
       do: :ok

  defp mark_superseded_records(
         target,
         runtime_opts,
         existing,
         new_id,
         fact_key,
         version,
         now,
         :version
       ) do
    existing
    |> Enum.reduce_while(:ok, fn record, :ok ->
      metadata_patch = %{
        "mem_os_conflict" => %{
          "reason" => "superseded",
          "superseded_by" => new_id,
          "fact_key" => fact_key,
          "at" => now
        }
      }

      attrs = Lifecycle.rewrite_record_with_metadata(record, metadata_patch)

      mem_os =
        case Metadata.from_record(record) do
          {:ok, decoded} ->
            decoded
            |> Map.put(:tier, :long)
            |> Map.put(:consolidation_version, version)
            |> Map.put(:last_accessed_at, now)

          {:error, _} ->
            %{tier: :long, consolidation_version: version, last_accessed_at: now}
        end

      case MemoryRuntime.remember(
             target,
             attrs,
             runtime_opts
             |> Keyword.put(:tier, :long)
             |> Keyword.put(:mem_os, mem_os)
             |> Keyword.put(:previous_tier, :long)
             |> Keyword.put(:strict_transition, false)
           ) do
        {:ok, _record} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec enforce_short_maintenance(state(), map() | struct(), keyword(), map()) :: state()
  defp enforce_short_maintenance(state, target, runtime_opts, short_context) do
    short_opts = Keyword.put(runtime_opts, :tier, :short)

    max_records = short_context.config.tiers.short.max_records
    query_limit = min(max(max_records * 2, max_records + 32), 1_000)

    case MemoryRuntime.recall(target, %{limit: query_limit, order: :desc}, short_opts) do
      {:ok, records} when length(records) > max_records ->
        overflow = length(records) - max_records

        records
        |> Enum.reverse()
        |> Enum.take(overflow)
        |> Enum.each(fn record ->
          _ = MemoryRuntime.forget(target, record.id, short_opts)
        end)

        state

      _ ->
        state
    end
  end

  @spec split_candidates(MapSet.t(candidate()), String.t()) :: {[candidate()], [candidate()]}
  defp split_candidates(candidates, namespace) do
    MapSet.to_list(candidates)
    |> Enum.split_with(fn {candidate_namespace, _id} -> candidate_namespace == namespace end)
  end

  @spec put_turn_counter(state(), String.t(), non_neg_integer()) :: state()
  defp put_turn_counter(state, namespace, next_turn_index) do
    %{state | turn_counters: Map.put(state.turn_counters, namespace, next_turn_index)}
  end

  @spec enqueue_short_candidate(state(), String.t(), String.t()) :: state()
  defp enqueue_short_candidate(state, namespace, id) do
    %{state | short_candidates: MapSet.put(state.short_candidates, {namespace, id})}
  end

  @spec next_consolidation_version(state()) :: {pos_integer(), state()}
  defp next_consolidation_version(state) do
    version = state.consolidation_version
    {version, %{state | consolidation_version: version + 1}}
  end

  @spec normalize_tier(term()) :: {:ok, Config.tier()} | {:error, term()}
  defp normalize_tier(:short), do: {:ok, :short}
  defp normalize_tier(:mid), do: {:ok, :mid}
  defp normalize_tier(:long), do: {:ok, :long}
  defp normalize_tier("short"), do: {:ok, :short}
  defp normalize_tier("mid"), do: {:ok, :mid}
  defp normalize_tier("long"), do: {:ok, :long}
  defp normalize_tier(other), do: {:error, {:invalid_tier, other}}

  @spec merge_ingest_metadata(map(), map()) :: map()
  defp merge_ingest_metadata(attrs, ingest_meta) do
    metadata = normalize_map(map_get(attrs, :metadata, %{}))
    mem_os_ingest = normalize_map(map_get(metadata, :mem_os_ingest, %{}))

    merged_metadata = Map.put(metadata, "mem_os_ingest", Map.merge(mem_os_ingest, ingest_meta))
    Map.put(attrs, :metadata, merged_metadata)
  end

  @spec record_debug(Jido.Memory.Record.t()) :: map()
  defp record_debug(record) do
    {:ok, mem_os} = Metadata.from_record(record)
    conflict = normalize_map(map_get(record.metadata || %{}, :mem_os_conflict, %{}))

    %{
      id: record.id,
      tier: mem_os.tier,
      chain_id: mem_os.chain_id,
      segment_id: mem_os.segment_id,
      page_id: mem_os.page_id,
      promotion_score: mem_os.promotion_score,
      conflict: conflict,
      fact_key: Lifecycle.fact_key(record)
    }
  end

  @spec to_jido_error(term(), atom()) :: term()
  defp to_jido_error(reason, operation) do
    if jido_error?(reason), do: reason, else: ErrorMapping.from_reason(reason, operation)
  end

  @spec jido_error?(term()) :: boolean()
  defp jido_error?(%Jido.Error.ValidationError{}), do: true
  defp jido_error?(%Jido.Error.ExecutionError{}), do: true
  defp jido_error?(%Jido.Error.RoutingError{}), do: true
  defp jido_error?(%Jido.Error.TimeoutError{}), do: true
  defp jido_error?(%Jido.Error.CompensationError{}), do: true
  defp jido_error?(%Jido.Error.InternalError{}), do: true
  defp jido_error?(_), do: false

  @spec with_app_config(keyword(), state()) :: keyword()
  defp with_app_config(runtime_opts, state) do
    Keyword.put_new(runtime_opts, :app_config, state.app_config)
  end

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
