defmodule Jido.MemoryOS.ReleaseController do
  @moduledoc """
  Phase 8 rollout controller for dual-run, cutover, rollback, and fallback.

  The controller keeps rollout state in one process and exposes explicit APIs for:
  - mode transitions (`:legacy`, `:dual_run`, `:memory_os`)
  - drift sampling between legacy and MemoryOS retrieval
  - cutover gating from drift thresholds
  - rollback triggers from SLO snapshots
  - post-rollback reconciliation
  """

  use GenServer

  alias Jido.Memory.Record
  alias Jido.Memory.Runtime
  alias Jido.MemoryOS.Migration

  @default_legacy_store {Jido.Memory.Store.ETS, [table: :jido_memory]}

  @default_rollback_thresholds %{
    latency_p95_ms: 450,
    error_rate: 0.03,
    queue_depth: 200
  }

  @type mode :: :legacy | :dual_run | :memory_os
  @type rollout_stage :: :internal | :canary | :global

  @type state :: %{
          mode: mode(),
          rollout_stage: rollout_stage(),
          memory_server: GenServer.server(),
          legacy_store: {module(), keyword()},
          fallback_enabled: boolean(),
          canary_ratio: float(),
          internal_groups: MapSet.t(String.t()),
          drift_threshold: float(),
          drift_min_samples: pos_integer(),
          drift_sample_limit: pos_integer(),
          drift_samples: [map()],
          rollback_thresholds: map(),
          last_rollback: map() | nil,
          metrics: map(),
          migration_opts: keyword()
        }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns current rollout mode.
  """
  @spec mode(GenServer.server()) :: {:ok, mode()}
  def mode(server \\ __MODULE__) do
    GenServer.call(server, :mode)
  end

  @doc """
  Returns controller status snapshot.
  """
  @spec status(GenServer.server()) :: {:ok, map()}
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  @doc """
  Updates rollout controls.

  Supported keys: `:rollout_stage`, `:canary_ratio`, `:internal_groups`, `:fallback_enabled`.
  """
  @spec configure_rollout(GenServer.server(), keyword()) :: {:ok, map()} | {:error, term()}
  def configure_rollout(server \\ __MODULE__, opts) when is_list(opts) do
    GenServer.call(server, {:configure_rollout, opts})
  end

  @doc """
  Sets active mode.

  Transition to `:memory_os` is cutover-gated by drift thresholds.
  """
  @spec set_mode(GenServer.server(), mode()) :: {:ok, mode()} | {:error, term()}
  def set_mode(server \\ __MODULE__, mode) do
    GenServer.call(server, {:set_mode, mode})
  end

  @doc """
  Returns drift summary used for cutover gating.
  """
  @spec drift_summary(GenServer.server()) :: {:ok, map()}
  def drift_summary(server \\ __MODULE__) do
    GenServer.call(server, :drift_summary)
  end

  @doc """
  Returns whether cutover gate currently passes.
  """
  @spec cutover_ready?(GenServer.server()) :: {:ok, boolean()}
  def cutover_ready?(server \\ __MODULE__) do
    GenServer.call(server, :cutover_ready)
  end

  @doc """
  Writes a memory record through the active rollout mode.
  """
  @spec remember(GenServer.server(), map() | struct(), map() | keyword(), keyword()) ::
          {:ok, Record.t()} | {:error, term()}
  def remember(server \\ __MODULE__, target, attrs, opts \\ []) do
    call_timeout = Keyword.get(opts, :call_timeout, :infinity)
    GenServer.call(server, {:remember, target, attrs, opts}, call_timeout)
  end

  @doc """
  Retrieves records through the active rollout mode.
  """
  @spec retrieve(GenServer.server(), map() | struct(), map() | keyword(), keyword()) ::
          {:ok, [Record.t()]} | {:error, term()}
  def retrieve(server \\ __MODULE__, target, query, opts \\ []) do
    call_timeout = Keyword.get(opts, :call_timeout, :infinity)
    GenServer.call(server, {:retrieve, target, query, opts}, call_timeout)
  end

  @doc """
  Evaluates rollback triggers from an SLO snapshot.

  Snapshot keys:
  - `:latency_p95_ms`
  - `:error_rate`
  - `:queue_depth`
  """
  @spec evaluate_rollback(GenServer.server(), map() | keyword()) ::
          {:ok, :stable | {:rollback, map()}}
  def evaluate_rollback(server \\ __MODULE__, snapshot) do
    GenServer.call(server, {:evaluate_rollback, snapshot})
  end

  @doc """
  Reconciles targets after rollback by migrating from legacy to MemoryOS.
  """
  @spec reconcile_post_rollback(GenServer.server(), [map() | struct()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_post_rollback(server \\ __MODULE__, targets, opts \\ []) when is_list(targets) do
    call_timeout = Keyword.get(opts, :call_timeout, :infinity)
    GenServer.call(server, {:reconcile_post_rollback, targets, opts}, call_timeout)
  end

  @impl true
  def init(opts) do
    state = %{
      mode: normalize_mode(Keyword.get(opts, :mode, :legacy), :legacy),
      rollout_stage: normalize_stage(Keyword.get(opts, :rollout_stage, :internal), :internal),
      memory_server: Keyword.get(opts, :memory_server, Jido.MemoryOS.MemoryManager),
      legacy_store: Keyword.get(opts, :legacy_store, @default_legacy_store),
      fallback_enabled: normalize_boolean(Keyword.get(opts, :fallback_enabled, true), true),
      canary_ratio: normalize_ratio(Keyword.get(opts, :canary_ratio, 0.1), 0.1),
      internal_groups:
        Keyword.get(opts, :internal_groups, ["internal"])
        |> normalize_string_list()
        |> MapSet.new(),
      drift_threshold: normalize_ratio(Keyword.get(opts, :drift_threshold, 0.2), 0.2),
      drift_min_samples: positive_int(Keyword.get(opts, :drift_min_samples, 20), 20),
      drift_sample_limit: positive_int(Keyword.get(opts, :drift_sample_limit, 400), 400),
      drift_samples: [],
      rollback_thresholds:
        normalize_rollback_thresholds(Keyword.get(opts, :rollback_thresholds, %{})),
      last_rollback: nil,
      metrics: %{
        remember_legacy: 0,
        remember_memory_os: 0,
        retrieve_legacy: 0,
        retrieve_memory_os: 0,
        dual_run_drift_samples: 0,
        fallbacks: 0,
        rollbacks: 0
      },
      migration_opts: Keyword.get(opts, :migration_opts, [])
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:mode, _from, state), do: {:reply, {:ok, state.mode}, state}

  @impl true
  def handle_call(:status, _from, state) do
    reply =
      %{
        mode: state.mode,
        rollout_stage: state.rollout_stage,
        fallback_enabled: state.fallback_enabled,
        canary_ratio: state.canary_ratio,
        internal_groups: MapSet.to_list(state.internal_groups),
        drift: summarize_drift(state),
        rollback_thresholds: state.rollback_thresholds,
        last_rollback: state.last_rollback,
        metrics: Map.put(state.metrics, :drift_samples, length(state.drift_samples))
      }

    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_call({:configure_rollout, opts}, _from, state) do
    next =
      state
      |> maybe_put_rollout_stage(opts)
      |> maybe_put_canary_ratio(opts)
      |> maybe_put_internal_groups(opts)
      |> maybe_put_fallback_enabled(opts)

    {:reply, {:ok, %{mode: next.mode, rollout_stage: next.rollout_stage}}, next}
  end

  @impl true
  def handle_call({:set_mode, mode}, _from, state) do
    normalized = normalize_mode(mode, :invalid)

    cond do
      normalized == :invalid ->
        {:reply, {:error, {:invalid_mode, mode}}, state}

      normalized == :memory_os ->
        summary = summarize_drift(state)

        if summary.ready? do
          {:reply, {:ok, :memory_os}, %{state | mode: :memory_os}}
        else
          {:reply, {:error, {:cutover_blocked, summary}}, state}
        end

      true ->
        {:reply, {:ok, normalized}, %{state | mode: normalized}}
    end
  end

  @impl true
  def handle_call(:drift_summary, _from, state),
    do: {:reply, {:ok, summarize_drift(state)}, state}

  @impl true
  def handle_call(:cutover_ready, _from, state) do
    {:reply, {:ok, summarize_drift(state).ready?}, state}
  end

  @impl true
  def handle_call({:remember, target, attrs, opts}, _from, state) do
    mode = effective_mode_for_target(state, target)

    case mode do
      :legacy ->
        result = legacy_remember(target, attrs, opts, state)
        {:reply, result, increment_metric(state, :remember_legacy)}

      :memory_os ->
        case memory_os_remember(target, attrs, opts, state) do
          {:ok, _record} = ok ->
            {:reply, ok, increment_metric(state, :remember_memory_os)}

          {:error, _reason} = error ->
            if state.fallback_enabled do
              fallback = legacy_remember(target, attrs, opts, state)
              {:reply, fallback, increment_metric(state, :fallbacks)}
            else
              {:reply, error, state}
            end
        end

      :dual_run ->
        legacy_result = legacy_remember(target, attrs, opts, state)
        memory_result = memory_os_remember(target, attrs, opts, state)

        next =
          state
          |> increment_metric(:remember_legacy)
          |> increment_metric(:remember_memory_os)

        reply = pick_dual_run_write_result(legacy_result, memory_result, state.fallback_enabled)
        {:reply, reply, next}
    end
  end

  @impl true
  def handle_call({:retrieve, target, query, opts}, _from, state) do
    mode = effective_mode_for_target(state, target)

    case mode do
      :legacy ->
        result = legacy_retrieve(target, query, opts, state)
        {:reply, result, increment_metric(state, :retrieve_legacy)}

      :memory_os ->
        case memory_os_retrieve(target, query, opts, state) do
          {:ok, _records} = ok ->
            {:reply, ok, increment_metric(state, :retrieve_memory_os)}

          {:error, _reason} = error ->
            if state.fallback_enabled do
              fallback = legacy_retrieve(target, query, opts, state)
              {:reply, fallback, increment_metric(state, :fallbacks)}
            else
              {:reply, error, state}
            end
        end

      :dual_run ->
        legacy_result = legacy_retrieve(target, query, opts, state)
        memory_result = memory_os_retrieve(target, query, opts, state)

        next =
          state
          |> increment_metric(:retrieve_legacy)
          |> increment_metric(:retrieve_memory_os)
          |> maybe_record_drift(target, query, legacy_result, memory_result)

        reply = pick_dual_run_read_result(legacy_result, memory_result, state.fallback_enabled)
        {:reply, reply, next}
    end
  end

  @impl true
  def handle_call({:evaluate_rollback, snapshot}, _from, state) do
    snapshot = normalize_snapshot(snapshot)

    if rollback_required?(state.mode, snapshot, state.rollback_thresholds) do
      reason = %{
        at: System.system_time(:millisecond),
        mode_before: state.mode,
        mode_after: :legacy,
        snapshot: snapshot,
        thresholds: state.rollback_thresholds
      }

      next =
        state
        |> Map.put(:mode, :legacy)
        |> Map.put(:last_rollback, reason)
        |> increment_metric(:rollbacks)

      {:reply, {:ok, {:rollback, reason}}, next}
    else
      {:reply, {:ok, :stable}, state}
    end
  end

  @impl true
  def handle_call({:reconcile_post_rollback, targets, opts}, _from, state) do
    if targets == [] do
      {:reply, {:error, :targets_required}, state}
    else
      merged_opts =
        state.migration_opts
        |> Keyword.merge(opts)
        |> Keyword.put_new(:legacy_store, state.legacy_store)
        |> Keyword.put_new(:server, state.memory_server)

      reports =
        Enum.map(targets, fn target ->
          case Migration.migrate_target(target, merged_opts) do
            {:ok, report} -> %{target: target_id(target), status: :ok, report: report}
            {:error, reason} -> %{target: target_id(target), status: :error, reason: reason}
          end
        end)

      summary = %{
        targets: length(targets),
        successful: Enum.count(reports, &(&1.status == :ok)),
        failed: Enum.count(reports, &(&1.status == :error)),
        migrated_count:
          reports
          |> Enum.filter(&(&1.status == :ok))
          |> Enum.reduce(0, fn %{report: report}, acc -> acc + report.migrated_count end),
        reports: reports
      }

      if summary.failed == 0 do
        {:reply, {:ok, summary}, state}
      else
        {:reply, {:error, {:reconciliation_failed, summary}}, state}
      end
    end
  end

  @spec pick_dual_run_write_result(term(), term(), boolean()) ::
          {:ok, Record.t()} | {:error, term()}
  defp pick_dual_run_write_result(legacy_result, memory_result, fallback_enabled?) do
    case {legacy_result, memory_result} do
      {{:ok, legacy}, {:ok, _memory}} -> {:ok, legacy}
      {{:ok, legacy}, {:error, _reason}} -> {:ok, legacy}
      {{:error, _legacy_reason}, {:ok, memory}} when fallback_enabled? -> {:ok, memory}
      {{:error, legacy_reason}, _} -> {:error, legacy_reason}
    end
  end

  @spec pick_dual_run_read_result(term(), term(), boolean()) ::
          {:ok, [Record.t()]} | {:error, term()}
  defp pick_dual_run_read_result(legacy_result, memory_result, fallback_enabled?) do
    case {legacy_result, memory_result} do
      {{:ok, legacy_records}, {:ok, _memory_records}} ->
        {:ok, legacy_records}

      {{:ok, legacy_records}, {:error, _reason}} ->
        {:ok, legacy_records}

      {{:error, _legacy_reason}, {:ok, memory_records}} when fallback_enabled? ->
        {:ok, memory_records}

      {{:error, legacy_reason}, _} ->
        {:error, legacy_reason}
    end
  end

  @spec legacy_remember(map() | struct(), map() | keyword(), keyword(), state()) ::
          {:ok, Record.t()} | {:error, term()}
  defp legacy_remember(target, attrs, opts, state) do
    runtime_opts = legacy_runtime_opts(target, opts, state)

    Runtime.remember(target, attrs, runtime_opts)
  end

  @spec memory_os_remember(map() | struct(), map() | keyword(), keyword(), state()) ::
          {:ok, Record.t()} | {:error, term()}
  defp memory_os_remember(target, attrs, opts, state) do
    memory_opts = memory_os_opts(opts, state)
    safe_call(fn -> Jido.MemoryOS.remember(target, attrs, memory_opts) end)
  end

  @spec legacy_retrieve(map() | struct(), map() | keyword(), keyword(), state()) ::
          {:ok, [Record.t()]} | {:error, term()}
  defp legacy_retrieve(target, query, opts, state) do
    query_map = normalize_map(query)

    runtime_query =
      query_map
      |> Map.put(:namespace, legacy_namespace(target, opts, state))
      |> Map.put(:store, state.legacy_store)

    Runtime.recall(target, runtime_query)
  end

  @spec memory_os_retrieve(map() | struct(), map() | keyword(), keyword(), state()) ::
          {:ok, [Record.t()]} | {:error, term()}
  defp memory_os_retrieve(target, query, opts, state) do
    memory_opts = memory_os_opts(opts, state)
    safe_call(fn -> Jido.MemoryOS.retrieve(target, query, memory_opts) end)
  end

  @spec memory_os_opts(keyword(), state()) :: keyword()
  defp memory_os_opts(opts, state) do
    opts
    |> Keyword.drop([:legacy_namespace, :legacy_store])
    |> Keyword.put(:server, state.memory_server)
    |> Keyword.put_new(:actor_id, Keyword.get(opts, :actor_id))
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  @spec legacy_runtime_opts(map() | struct(), keyword(), state()) :: keyword()
  defp legacy_runtime_opts(target, opts, state) do
    [
      namespace: legacy_namespace(target, opts, state),
      store: Keyword.get(opts, :legacy_store, state.legacy_store)
    ]
  end

  @spec legacy_namespace(map() | struct(), keyword(), state()) :: String.t()
  defp legacy_namespace(target, opts, _state) do
    case Keyword.get(opts, :legacy_namespace) do
      namespace when is_binary(namespace) and namespace != "" ->
        namespace

      _ ->
        case target_id(target) do
          nil -> "agent:unknown"
          id -> "agent:" <> id
        end
    end
  end

  @spec safe_call((-> {:ok, term()} | {:error, term()})) :: {:ok, term()} | {:error, term()}
  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error -> {:error, {:exception, error, __STACKTRACE__}}
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end

  @spec maybe_record_drift(state(), map() | struct(), map() | keyword(), term(), term()) ::
          state()
  defp maybe_record_drift(state, target, query, legacy_result, memory_result) do
    case {legacy_result, memory_result} do
      {{:ok, legacy_records}, {:ok, memory_records}} ->
        sample =
          build_drift_sample(
            target,
            query,
            legacy_records,
            memory_records,
            state.drift_threshold,
            degraded?: false
          )

        state
        |> append_drift_sample(sample)
        |> increment_metric(:dual_run_drift_samples)

      {{:ok, legacy_records}, {:error, _reason}} ->
        sample =
          build_drift_sample(
            target,
            query,
            legacy_records,
            [],
            state.drift_threshold,
            degraded?: true
          )

        state
        |> append_drift_sample(sample)
        |> increment_metric(:dual_run_drift_samples)

      _ ->
        state
    end
  end

  @spec build_drift_sample(
          map() | struct(),
          map() | keyword(),
          [Record.t()],
          [Record.t()],
          float(),
          keyword()
        ) :: map()
  defp build_drift_sample(target, query, legacy_records, memory_records, threshold, opts) do
    legacy_set = MapSet.new(Enum.map(legacy_records, &record_signature/1))
    memory_set = MapSet.new(Enum.map(memory_records, &record_signature/1))

    union = MapSet.union(legacy_set, memory_set) |> MapSet.size()
    intersection = MapSet.intersection(legacy_set, memory_set) |> MapSet.size()

    drift = if union == 0, do: 0.0, else: 1.0 - intersection / union

    %{
      at: System.system_time(:millisecond),
      target_id: target_id(target),
      query_hash: :erlang.phash2(normalize_map(query)),
      legacy_count: length(legacy_records),
      memory_os_count: length(memory_records),
      drift: Float.round(drift, 6),
      threshold: threshold,
      exceeds_threshold?: drift > threshold,
      degraded?: Keyword.get(opts, :degraded?, false)
    }
  end

  @spec summarize_drift(state()) :: map()
  defp summarize_drift(state) do
    samples = state.drift_samples
    count = length(samples)

    avg =
      if count == 0 do
        0.0
      else
        Enum.reduce(samples, 0.0, fn sample, acc -> acc + sample.drift end) / count
      end

    max =
      if count == 0 do
        0.0
      else
        Enum.max_by(samples, & &1.drift).drift
      end

    over_threshold = Enum.count(samples, & &1.exceeds_threshold?)

    %{
      samples: count,
      min_samples: state.drift_min_samples,
      threshold: state.drift_threshold,
      average: Float.round(avg, 6),
      max: Float.round(max, 6),
      over_threshold: over_threshold,
      ready?: count >= state.drift_min_samples and max <= state.drift_threshold
    }
  end

  @spec append_drift_sample(state(), map()) :: state()
  defp append_drift_sample(state, sample) do
    updated = [sample | state.drift_samples] |> Enum.take(state.drift_sample_limit)
    %{state | drift_samples: updated}
  end

  @spec rollback_required?(mode(), map(), map()) :: boolean()
  defp rollback_required?(:memory_os, snapshot, thresholds) do
    snapshot.latency_p95_ms > thresholds.latency_p95_ms or
      snapshot.error_rate > thresholds.error_rate or
      snapshot.queue_depth > thresholds.queue_depth
  end

  defp rollback_required?(_mode, _snapshot, _thresholds), do: false

  @spec normalize_snapshot(map() | keyword()) :: map()
  defp normalize_snapshot(snapshot) do
    map = normalize_map(snapshot)

    %{
      latency_p95_ms:
        non_negative_number(
          Map.get(map, :latency_p95_ms, Map.get(map, "latency_p95_ms")),
          0
        ),
      error_rate:
        normalize_ratio(Map.get(map, :error_rate, Map.get(map, "error_rate", 0.0)), 0.0),
      queue_depth:
        non_negative_integer(Map.get(map, :queue_depth, Map.get(map, "queue_depth")), 0)
    }
  end

  @spec maybe_put_rollout_stage(state(), keyword()) :: state()
  defp maybe_put_rollout_stage(state, opts) do
    if Keyword.has_key?(opts, :rollout_stage) do
      %{
        state
        | rollout_stage: normalize_stage(Keyword.get(opts, :rollout_stage), state.rollout_stage)
      }
    else
      state
    end
  end

  @spec maybe_put_canary_ratio(state(), keyword()) :: state()
  defp maybe_put_canary_ratio(state, opts) do
    if Keyword.has_key?(opts, :canary_ratio) do
      %{
        state
        | canary_ratio: normalize_ratio(Keyword.get(opts, :canary_ratio), state.canary_ratio)
      }
    else
      state
    end
  end

  @spec maybe_put_internal_groups(state(), keyword()) :: state()
  defp maybe_put_internal_groups(state, opts) do
    if Keyword.has_key?(opts, :internal_groups) do
      groups = Keyword.get(opts, :internal_groups, []) |> normalize_string_list() |> MapSet.new()
      %{state | internal_groups: groups}
    else
      state
    end
  end

  @spec maybe_put_fallback_enabled(state(), keyword()) :: state()
  defp maybe_put_fallback_enabled(state, opts) do
    if Keyword.has_key?(opts, :fallback_enabled) do
      %{
        state
        | fallback_enabled:
            normalize_boolean(Keyword.get(opts, :fallback_enabled), state.fallback_enabled)
      }
    else
      state
    end
  end

  @spec effective_mode_for_target(state(), map() | struct()) :: mode()
  defp effective_mode_for_target(state, target) do
    if rollout_enabled_for_target?(state, target) do
      state.mode
    else
      :legacy
    end
  end

  @spec rollout_enabled_for_target?(state(), map() | struct()) :: boolean()
  defp rollout_enabled_for_target?(state, target) do
    group = target_group(target)
    id = target_id(target)

    case state.rollout_stage do
      :global ->
        true

      :internal ->
        MapSet.member?(state.internal_groups, group)

      :canary ->
        MapSet.member?(state.internal_groups, group) or stable_hash_ratio(id) < state.canary_ratio
    end
  end

  @spec target_group(map() | struct()) :: String.t()
  defp target_group(%{group: group}) when is_binary(group), do: group
  defp target_group(%{"group" => group}) when is_binary(group), do: group
  defp target_group(_), do: "external"

  @spec stable_hash_ratio(String.t() | nil) :: float()
  defp stable_hash_ratio(nil), do: 0.0

  defp stable_hash_ratio(value) do
    :erlang.phash2(value, 10_000) / 10_000
  end

  @spec target_id(map() | struct()) :: String.t() | nil
  defp target_id(%{id: id}) when is_binary(id), do: id
  defp target_id(%{"id" => id}) when is_binary(id), do: id
  defp target_id(%{agent: %{id: id}}) when is_binary(id), do: id
  defp target_id(_), do: nil

  @spec record_signature(Record.t()) :: String.t()
  defp record_signature(%Record{id: id, text: text}) do
    normalized_text = (text || "") |> String.downcase() |> String.trim()
    "#{id}|#{normalized_text}"
  end

  @spec increment_metric(state(), atom()) :: state()
  defp increment_metric(state, key) do
    update_in(state, [:metrics, key], fn current -> (current || 0) + 1 end)
  end

  @spec normalize_rollback_thresholds(map() | keyword()) :: map()
  defp normalize_rollback_thresholds(thresholds) do
    input = normalize_map(thresholds)

    %{
      latency_p95_ms:
        positive_int(
          Map.get(input, :latency_p95_ms, Map.get(input, "latency_p95_ms")),
          @default_rollback_thresholds.latency_p95_ms
        ),
      error_rate:
        normalize_ratio(
          Map.get(input, :error_rate, Map.get(input, "error_rate")),
          @default_rollback_thresholds.error_rate
        ),
      queue_depth:
        positive_int(
          Map.get(input, :queue_depth, Map.get(input, "queue_depth")),
          @default_rollback_thresholds.queue_depth
        )
    }
  end

  @spec normalize_mode(term(), mode() | :invalid) :: mode() | :invalid
  defp normalize_mode(:legacy, _fallback), do: :legacy
  defp normalize_mode(:dual_run, _fallback), do: :dual_run
  defp normalize_mode(:memory_os, _fallback), do: :memory_os
  defp normalize_mode("legacy", _fallback), do: :legacy
  defp normalize_mode("dual_run", _fallback), do: :dual_run
  defp normalize_mode("memory_os", _fallback), do: :memory_os
  defp normalize_mode(_value, fallback), do: fallback

  @spec normalize_stage(term(), rollout_stage()) :: rollout_stage()
  defp normalize_stage(:internal, _fallback), do: :internal
  defp normalize_stage(:canary, _fallback), do: :canary
  defp normalize_stage(:global, _fallback), do: :global
  defp normalize_stage("internal", _fallback), do: :internal
  defp normalize_stage("canary", _fallback), do: :canary
  defp normalize_stage("global", _fallback), do: :global
  defp normalize_stage(_value, fallback), do: fallback

  @spec normalize_string_list(term()) :: [String.t()]
  defp normalize_string_list(list) when is_list(list) do
    list
    |> Enum.filter(&(!is_nil(&1)))
    |> Enum.map(fn
      value when is_binary(value) -> String.trim(value)
      value when is_atom(value) -> value |> Atom.to_string() |> String.trim()
      value -> value |> to_string() |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_string_list(value) when is_binary(value), do: [String.trim(value)]
  defp normalize_string_list(_), do: []

  @spec normalize_boolean(term(), boolean()) :: boolean()
  defp normalize_boolean(value, _fallback) when is_boolean(value), do: value
  defp normalize_boolean("true", _fallback), do: true
  defp normalize_boolean("false", _fallback), do: false
  defp normalize_boolean(_value, fallback), do: fallback

  @spec positive_int(term(), pos_integer()) :: pos_integer()
  defp positive_int(value, _fallback) when is_integer(value) and value > 0, do: value
  defp positive_int(_value, fallback), do: fallback

  @spec non_negative_integer(term(), non_neg_integer()) :: non_neg_integer()
  defp non_negative_integer(value, _fallback) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value, fallback), do: fallback

  @spec non_negative_number(term(), number()) :: number()
  defp non_negative_number(value, _fallback) when is_number(value) and value >= 0, do: value
  defp non_negative_number(_value, fallback), do: fallback

  @spec normalize_ratio(term(), float()) :: float()
  defp normalize_ratio(value, _fallback) when is_number(value) and value >= 0 and value <= 1,
    do: value * 1.0

  defp normalize_ratio(_value, fallback), do: fallback

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}
end
