defmodule Jido.MemoryOS.MemoryManager do
  @moduledoc """
  Phase 3 control-plane orchestrator for ingestion, retrieval, consolidation,
  and maintenance operations.
  """

  use GenServer

  alias Jido.MemoryOS.Adapter.MemoryRuntime
  alias Jido.MemoryOS.{Config, ErrorMapping, Lifecycle, Metadata}

  @type candidate :: {String.t(), String.t()}
  @type operation :: :remember | :retrieve | :explain_retrieval | :forget | :prune | :consolidate

  @type request :: %{
          id: String.t(),
          op: operation(),
          target: map() | struct(),
          payload: term(),
          runtime_opts: keyword(),
          from: GenServer.from() | nil,
          agent_key: String.t(),
          enqueued_at: integer(),
          deadline_ms: integer(),
          trace_id: String.t(),
          internal?: boolean()
        }

  @type retrieval_feature :: %{
          lexical: number(),
          semantic: number(),
          recency: number(),
          heat: number(),
          persona: number(),
          tier_bias: number(),
          final_score: number()
        }

  @type state :: %{
          app_config: map(),
          policy_cache: Config.t(),
          short_candidates: MapSet.t(candidate()),
          long_candidates: MapSet.t(candidate()),
          turn_counters: %{optional(String.t()) => non_neg_integer()},
          consolidation_version: pos_integer(),
          last_conflicts: [map()],
          queue_by_agent: %{optional(String.t()) => :queue.queue(request())},
          agent_order: :queue.queue(String.t()),
          scheduled_agents: MapSet.t(String.t()),
          queue_depth: non_neg_integer(),
          processing: boolean(),
          dead_letters: [map()],
          metrics: map(),
          pending_consolidation: %{
            optional(String.t()) => %{
              ref: reference(),
              timer_ref: reference(),
              target: term(),
              opts: keyword()
            }
          }
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
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:remember, target, attrs, runtime_opts}, call_timeout)
  end

  @doc """
  Retrieves records by query.
  """
  @spec retrieve(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  def retrieve(target, query, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:retrieve, target, query, runtime_opts}, call_timeout)
  end

  @doc """
  Deletes one record by id.
  """
  @spec forget(map() | struct(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def forget(target, id, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:forget, target, id, runtime_opts}, call_timeout)
  end

  @doc """
  Prunes expired records from the selected store.
  """
  @spec prune(map() | struct(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune(target, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:prune, target, runtime_opts}, call_timeout)
  end

  @doc """
  Runs lifecycle consolidation for short->mid->long tier transitions.
  """
  @spec consolidate(map() | struct(), keyword()) :: {:ok, map()} | {:error, term()}
  def consolidate(target, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:consolidate, target, runtime_opts}, call_timeout)
  end

  @doc """
  Returns retrieval diagnostics payload.
  """
  @spec explain_retrieval(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def explain_retrieval(target, query, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:explain_retrieval, target, query, runtime_opts}, call_timeout)
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

  @doc """
  Returns dead-lettered ingestion entries.
  """
  @spec dead_letters(GenServer.server()) :: {:ok, [map()]}
  def dead_letters(server \\ __MODULE__) do
    GenServer.call(server, :dead_letters)
  end

  @doc """
  Returns manager queue/operation metrics.
  """
  @spec metrics(GenServer.server()) :: {:ok, map()}
  def metrics(server \\ __MODULE__) do
    GenServer.call(server, :metrics)
  end

  @doc """
  Cancels queued requests for one agent and optional operation.
  """
  @spec cancel_pending(GenServer.server(), keyword()) :: {:ok, non_neg_integer()}
  def cancel_pending(server \\ __MODULE__, opts \\ []) do
    GenServer.call(server, {:cancel_pending, opts})
  end

  @impl true
  def init(opts) do
    app_config = Keyword.get(opts, :app_config, Config.app_config())

    case Config.validate(app_config) do
      {:ok, config} ->
        {:ok,
         %{
           app_config: app_config,
           policy_cache: config,
           short_candidates: MapSet.new(),
           long_candidates: MapSet.new(),
           turn_counters: %{},
           consolidation_version: 1,
           last_conflicts: [],
           queue_by_agent: %{},
           agent_order: :queue.new(),
           scheduled_agents: MapSet.new(),
           queue_depth: 0,
           processing: false,
           dead_letters: [],
           metrics: %{
             queued: 0,
             processed: 0,
             timed_out: 0,
             overloaded: 0,
             retried: 0,
             dead_lettered: 0,
             cancelled: 0
           },
           pending_consolidation: %{}
         }}

      {:error, reason} ->
        {:stop, ErrorMapping.from_reason(reason, :init)}
    end
  end

  @impl true
  def handle_call({:remember, target, attrs, runtime_opts}, from, state) do
    enqueue_reply_request(:remember, target, attrs, runtime_opts, from, state)
  end

  @impl true
  def handle_call({:retrieve, target, query, runtime_opts}, from, state) do
    enqueue_reply_request(:retrieve, target, query, runtime_opts, from, state)
  end

  @impl true
  def handle_call({:explain_retrieval, target, query, runtime_opts}, from, state) do
    enqueue_reply_request(:explain_retrieval, target, query, runtime_opts, from, state)
  end

  @impl true
  def handle_call({:forget, target, id, runtime_opts}, from, state) do
    enqueue_reply_request(:forget, target, id, runtime_opts, from, state)
  end

  @impl true
  def handle_call({:prune, target, runtime_opts}, from, state) do
    enqueue_reply_request(:prune, target, :none, runtime_opts, from, state)
  end

  @impl true
  def handle_call({:consolidate, target, runtime_opts}, from, state) do
    enqueue_reply_request(:consolidate, target, :none, runtime_opts, from, state)
  end

  @impl true
  def handle_call(:current_config, _from, state) do
    {:reply, {:ok, state.policy_cache}, state}
  end

  @impl true
  def handle_call(:last_conflicts, _from, state) do
    {:reply, {:ok, state.last_conflicts}, state}
  end

  @impl true
  def handle_call(:dead_letters, _from, state) do
    {:reply, {:ok, state.dead_letters}, state}
  end

  @impl true
  def handle_call(:metrics, _from, state) do
    queue_metrics = %{
      queue_depth: state.queue_depth,
      pending_agents: map_size(state.queue_by_agent)
    }

    {:reply, {:ok, Map.merge(state.metrics, queue_metrics)}, state}
  end

  @impl true
  def handle_call({:cancel_pending, opts}, _from, state) do
    agent_filter =
      normalize_agent_key(Keyword.get(opts, :agent_id) || Keyword.get(opts, :agent_key))

    op_filter =
      case Keyword.get(opts, :operation) do
        nil ->
          :any

        op when op in [:remember, :retrieve, :explain_retrieval, :forget, :prune, :consolidate] ->
          op

        _ ->
          :any
      end

    {cancelled_requests, new_state} = cancel_matching_requests(state, agent_filter, op_filter)

    Enum.each(cancelled_requests, fn request ->
      if request.from do
        GenServer.reply(
          request.from,
          {:error,
           Jido.Error.execution_error("request cancelled",
             phase: :execution,
             details: %{code: :cancelled, operation: request.op, trace_id: request.trace_id}
           )}
        )
      end
    end)

    cancelled_count = length(cancelled_requests)

    {:reply, {:ok, cancelled_count}, increment_metric(new_state, :cancelled, cancelled_count)}
  end

  @impl true
  def handle_info(:drain_queue, %{processing: true} = state), do: {:noreply, state}

  @impl true
  def handle_info(:drain_queue, state) do
    case dequeue_next_request(state) do
      {:empty, state1} ->
        {:noreply, %{state1 | processing: false}}

      {:ok, request, state1} ->
        state2 = %{state1 | processing: true}

        {result, state3} = execute_request(request, state2)

        state4 =
          state3
          |> maybe_reply(request.from, result)
          |> increment_metric(:processed)
          |> Map.put(:processing, false)

        if state4.queue_depth > 0 do
          send(self(), :drain_queue)
        end

        {:noreply, state4}
    end
  end

  @impl true
  def handle_info({:auto_consolidate, namespace, ref}, state) do
    case Map.get(state.pending_consolidation, namespace) do
      %{ref: ^ref, target: target, opts: opts} ->
        state1 = %{
          state
          | pending_consolidation: Map.delete(state.pending_consolidation, namespace)
        }

        case enqueue_request(:consolidate, target, :none, opts, nil, state1, internal?: true) do
          {:ok, state2} ->
            send(self(), :drain_queue)
            {:noreply, state2}

          {:error, _reason, state2} ->
            {:noreply, state2}
        end

      _ ->
        {:noreply, state}
    end
  end

  @spec enqueue_reply_request(
          operation(),
          map() | struct(),
          term(),
          keyword(),
          GenServer.from(),
          state()
        ) ::
          {:noreply, state()} | {:reply, term(), state()}
  defp enqueue_reply_request(op, target, payload, runtime_opts, from, state) do
    runtime_opts = with_app_config(runtime_opts, state)

    case enqueue_request(op, target, payload, runtime_opts, from, state) do
      {:ok, state1} ->
        send(self(), :drain_queue)
        {:noreply, state1}

      {:error, reason, state1} ->
        {:reply, {:error, reason}, state1}
    end
  end

  @spec enqueue_request(
          operation(),
          map() | struct(),
          term(),
          keyword(),
          GenServer.from() | nil,
          state(),
          keyword()
        ) ::
          {:ok, state()} | {:error, term(), state()}
  defp enqueue_request(op, target, payload, runtime_opts, from, state, opts \\ []) do
    now = System.system_time(:millisecond)

    timeout_ms =
      normalize_timeout(
        Keyword.get(runtime_opts, :timeout_ms),
        state.policy_cache.manager.request_timeout_ms
      )

    trace_id = normalize_trace_id(Keyword.get(runtime_opts, :correlation_id))
    agent_key = request_agent_key(target, runtime_opts)
    internal? = Keyword.get(opts, :internal?, false)

    request = %{
      id: "rq-" <> Integer.to_string(System.unique_integer([:positive, :monotonic])),
      op: op,
      target: target,
      payload: payload,
      runtime_opts: runtime_opts,
      from: from,
      agent_key: agent_key,
      enqueued_at: now,
      deadline_ms: now + timeout_ms,
      trace_id: trace_id,
      internal?: internal?
    }

    with :ok <- ensure_queue_capacity(state, agent_key, internal?, trace_id) do
      {:ok,
       state
       |> put_request_in_queue(agent_key, request)
       |> increment_metric(:queued)}
    else
      {:error, overload} ->
        {:error, overload, increment_metric(state, :overloaded)}
    end
  end

  @spec ensure_queue_capacity(state(), String.t(), boolean(), String.t()) ::
          :ok | {:error, term()}
  defp ensure_queue_capacity(_state, _agent_key, true, _trace_id), do: :ok

  defp ensure_queue_capacity(state, agent_key, false, trace_id) do
    manager_cfg = state.policy_cache.manager
    queue_depth = state.queue_depth
    agent_depth = state.queue_by_agent |> Map.get(agent_key, :queue.new()) |> :queue.len()

    cond do
      queue_depth >= manager_cfg.queue_max_depth ->
        {:error,
         Jido.Error.execution_error("memory manager queue overloaded",
           phase: :execution,
           details: %{
             code: :manager_overloaded,
             queue_depth: queue_depth,
             queue_max_depth: manager_cfg.queue_max_depth,
             retry_after_ms: max(50, div(manager_cfg.request_timeout_ms, 4)),
             trace_id: trace_id
           }
         )}

      agent_depth >= manager_cfg.queue_per_agent ->
        {:error,
         Jido.Error.execution_error("memory manager per-agent queue limit reached",
           phase: :execution,
           details: %{
             code: :manager_agent_overloaded,
             agent_key: agent_key,
             agent_queue_depth: agent_depth,
             queue_per_agent: manager_cfg.queue_per_agent,
             retry_after_ms: max(50, div(manager_cfg.request_timeout_ms, 4)),
             trace_id: trace_id
           }
         )}

      true ->
        :ok
    end
  end

  @spec put_request_in_queue(state(), String.t(), request()) :: state()
  defp put_request_in_queue(state, agent_key, request) do
    agent_queue = Map.get(state.queue_by_agent, agent_key, :queue.new())
    was_empty? = :queue.is_empty(agent_queue)
    updated_queue = :queue.in(request, agent_queue)

    {agent_order, scheduled_agents} =
      if was_empty? and not MapSet.member?(state.scheduled_agents, agent_key) do
        {:queue.in(agent_key, state.agent_order), MapSet.put(state.scheduled_agents, agent_key)}
      else
        {state.agent_order, state.scheduled_agents}
      end

    %{
      state
      | queue_by_agent: Map.put(state.queue_by_agent, agent_key, updated_queue),
        agent_order: agent_order,
        scheduled_agents: scheduled_agents,
        queue_depth: state.queue_depth + 1
    }
  end

  @spec dequeue_next_request(state()) :: {:empty, state()} | {:ok, request(), state()}
  defp dequeue_next_request(state) do
    case :queue.out(state.agent_order) do
      {:empty, _} ->
        {:empty, state}

      {{:value, agent_key}, rest_order} ->
        case Map.get(state.queue_by_agent, agent_key, :queue.new()) |> :queue.out() do
          {:empty, _} ->
            state1 = %{
              state
              | queue_by_agent: Map.delete(state.queue_by_agent, agent_key),
                agent_order: rest_order,
                scheduled_agents: MapSet.delete(state.scheduled_agents, agent_key)
            }

            dequeue_next_request(state1)

          {{:value, request}, remaining_agent_queue} ->
            {queue_by_agent, agent_order, scheduled_agents} =
              if :queue.is_empty(remaining_agent_queue) do
                {
                  Map.delete(state.queue_by_agent, agent_key),
                  rest_order,
                  MapSet.delete(state.scheduled_agents, agent_key)
                }
              else
                {
                  Map.put(state.queue_by_agent, agent_key, remaining_agent_queue),
                  :queue.in(agent_key, rest_order),
                  state.scheduled_agents
                }
              end

            {:ok, request,
             %{
               state
               | queue_by_agent: queue_by_agent,
                 agent_order: agent_order,
                 scheduled_agents: scheduled_agents,
                 queue_depth: max(0, state.queue_depth - 1)
             }}
        end
    end
  end

  @spec cancel_matching_requests(state(), String.t() | :any, operation() | :any) ::
          {[request()], state()}
  defp cancel_matching_requests(state, agent_filter, op_filter) do
    Enum.reduce(state.queue_by_agent, {[], %{state | queue_by_agent: %{}, queue_depth: 0}}, fn
      {agent_key, queue}, {cancelled, acc_state} ->
        {keep_queue, drop_requests} =
          split_queue(queue, fn request ->
            cancel_match?(request, agent_filter, op_filter, agent_key)
          end)

        acc_state =
          if :queue.is_empty(keep_queue) do
            acc_state
          else
            put_queue_without_limit(acc_state, agent_key, keep_queue)
          end

        {drop_requests ++ cancelled, acc_state}
    end)
    |> then(fn {cancelled, acc_state} ->
      rebuilt_order =
        acc_state.queue_by_agent
        |> Map.keys()
        |> Enum.reduce(:queue.new(), fn key, q -> :queue.in(key, q) end)

      {
        cancelled,
        %{
          acc_state
          | agent_order: rebuilt_order,
            scheduled_agents: MapSet.new(Map.keys(acc_state.queue_by_agent))
        }
      }
    end)
  end

  @spec split_queue(:queue.queue(request()), (request() -> boolean())) ::
          {:queue.queue(request()), [request()]}
  defp split_queue(queue, predicate) do
    queue
    |> :queue.to_list()
    |> Enum.reduce({:queue.new(), []}, fn request, {keep, drop} ->
      if predicate.(request) do
        {keep, [request | drop]}
      else
        {:queue.in(request, keep), drop}
      end
    end)
    |> then(fn {keep, drop} -> {keep, Enum.reverse(drop)} end)
  end

  @spec put_queue_without_limit(state(), String.t(), :queue.queue(request())) :: state()
  defp put_queue_without_limit(state, agent_key, queue) do
    %{
      state
      | queue_by_agent: Map.put(state.queue_by_agent, agent_key, queue),
        queue_depth: state.queue_depth + :queue.len(queue)
    }
  end

  @spec cancel_match?(request(), String.t() | :any, operation() | :any, String.t()) :: boolean()
  defp cancel_match?(_request, :any, :any, _agent_key), do: true

  defp cancel_match?(request, agent_filter, op_filter, agent_key) do
    agent_ok = agent_filter == :any or agent_filter == agent_key
    op_ok = op_filter == :any or op_filter == request.op
    agent_ok and op_ok
  end

  @spec execute_request(request(), state()) :: {term(), state()}
  defp execute_request(request, state) do
    now = System.system_time(:millisecond)

    if now > request.deadline_ms do
      timeout_result(request, state)
    else
      sleep_ms =
        max(0, normalize_integer(Keyword.get(request.runtime_opts, :operation_sleep_ms), 0))

      if sleep_ms > 0, do: Process.sleep(sleep_ms)

      if System.system_time(:millisecond) > request.deadline_ms do
        timeout_result(request, state)
      else
        case request.op do
          :remember -> remember_request(request, state)
          :retrieve -> retrieve_request(request, state, false)
          :explain_retrieval -> retrieve_request(request, state, true)
          :forget -> forget_request(request, state)
          :prune -> prune_request(request, state)
          :consolidate -> consolidate_request(request, state)
        end
      end
    end
  end

  @spec timeout_result(request(), state()) :: {term(), state()}
  defp timeout_result(request, state) do
    timeout = max(0, request.deadline_ms - request.enqueued_at)

    result =
      {:error,
       Jido.Error.timeout_error("memory manager request timed out",
         timeout: timeout,
         details: %{code: :manager_timeout, operation: request.op, trace_id: request.trace_id}
       )}

    {result, increment_metric(state, :timed_out)}
  end

  @spec remember_request(request(), state()) :: {term(), state()}
  defp remember_request(request, state) do
    runtime_opts = request.runtime_opts

    case normalize_tier(Keyword.get(runtime_opts, :tier, :short)) do
      {:ok, :short} ->
        {result, state1} =
          remember_short(request.target, request.payload, runtime_opts, state, request.trace_id)

        {result, state1}

      {:ok, _tier} ->
        result =
          retry_runtime_remember(
            request.target,
            request.payload,
            runtime_opts,
            state,
            :remember,
            request.trace_id
          )

        case result do
          {{:ok, record}, state1} -> {{:ok, record}, state1}
          {{:error, reason}, state1} -> {{:error, to_jido_error(reason, :remember)}, state1}
        end

      {:error, reason} ->
        {{:error, ErrorMapping.from_reason(reason, :remember)}, state}
    end
  end

  @spec retrieve_request(request(), state(), boolean()) :: {term(), state()}
  defp retrieve_request(request, state, explain?) do
    with {:ok, query_map} <- to_query_map(request.payload),
         {:ok, retrieval} <-
           run_retrieval_pipeline(request.target, query_map, request.runtime_opts, state) do
      if explain? do
        {:ok,
         %{
           query: query_map,
           result_count: length(retrieval.records),
           retrieval: state.policy_cache.retrieval,
           lifecycle: state.policy_cache.lifecycle,
           manager: state.policy_cache.manager,
           tier_mode: retrieval.tier_mode,
           queue: %{depth: state.queue_depth, pending_agents: map_size(state.queue_by_agent)},
           records: Enum.map(retrieval.records, &record_debug/1),
           scored_candidates: retrieval.scored_candidates,
           excluded: retrieval.excluded,
           selection_rationale: retrieval.selection_rationale,
           recent_conflicts: state.last_conflicts
         }}
        |> then(&{&1, state})
      else
        {{:ok, retrieval.records}, state}
      end
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, if(explain?, do: :explain_retrieval, else: :retrieve))},
         state}
    end
  end

  @spec forget_request(request(), state()) :: {term(), state()}
  defp forget_request(request, state) do
    result = MemoryRuntime.forget(request.target, request.payload, request.runtime_opts)
    {result, state}
  end

  @spec prune_request(request(), state()) :: {term(), state()}
  defp prune_request(request, state) do
    result = MemoryRuntime.prune(request.target, request.runtime_opts)
    {result, state}
  end

  @spec consolidate_request(request(), state()) :: {term(), state()}
  defp consolidate_request(request, state) do
    {result, state1} = run_consolidation(request.target, request.runtime_opts, state)
    {result, state1}
  end

  @spec run_retrieval_pipeline(map() | struct(), map(), keyword(), state()) ::
          {:ok, map()} | {:error, term()}
  defp run_retrieval_pipeline(target, query_map, runtime_opts, state) do
    tier_mode = resolve_tier_mode(query_map, runtime_opts)
    tiers = mode_tiers(tier_mode)

    limit =
      normalize_integer(
        map_get(query_map, :limit, state.policy_cache.retrieval.limit),
        state.policy_cache.retrieval.limit
      )

    now = System.system_time(:millisecond)

    base_query =
      query_map
      |> Map.drop([
        :tier_mode,
        "tier_mode",
        :persona_keys,
        "persona_keys",
        :debug,
        "debug",
        :include_excluded,
        "include_excluded"
      ])
      |> Map.put(:limit, max(limit * max(length(tiers), 1), limit))

    with {:ok, fetched} <- fetch_tier_candidates(target, base_query, runtime_opts, tiers),
         {:ok, ranked} <- rank_candidates(fetched, query_map, state.policy_cache.retrieval, now) do
      selected = ranked |> Enum.take(limit)
      selected_keys = MapSet.new(Enum.map(selected, &candidate_key(&1.record)))

      excluded =
        ranked
        |> Enum.reject(&(candidate_key(&1.record) in selected_keys))
        |> Enum.map(fn candidate ->
          %{
            id: candidate.record.id,
            namespace: candidate.record.namespace,
            tier: candidate.tier,
            reason: :below_rank_limit,
            final_score: candidate.features.final_score
          }
        end)

      {:ok,
       %{
         records: Enum.map(selected, & &1.record),
         tier_mode: tier_mode,
         scored_candidates:
           Enum.map(selected, fn candidate ->
             %{
               id: candidate.record.id,
               namespace: candidate.record.namespace,
               tier: candidate.tier,
               features: candidate.features,
               include_reason: :selected
             }
           end),
         excluded: excluded,
         selection_rationale: %{
           tie_breaker: "final_score desc, observed_at desc, id asc",
           weights: %{
             lexical: state.policy_cache.retrieval.ranking.lexical_weight,
             semantic: state.policy_cache.retrieval.ranking.semantic_weight,
             recency_bonus: 0.15,
             tier_bias: 0.1
           }
         }
       }}
    end
  end

  @spec fetch_tier_candidates(map() | struct(), map(), keyword(), [Config.tier()]) ::
          {:ok, [map()]} | {:error, term()}
  defp fetch_tier_candidates(target, query_map, runtime_opts, tiers) do
    tiers
    |> Enum.reduce_while({:ok, []}, fn tier, {:ok, acc} ->
      tier_opts = Keyword.put(runtime_opts, :tier, tier)

      case MemoryRuntime.recall(target, query_map, tier_opts) do
        {:ok, records} ->
          tagged = Enum.map(records, &%{tier: tier, record: &1})
          {:cont, {:ok, tagged ++ acc}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, tagged} -> {:ok, dedupe_candidates(tagged)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec dedupe_candidates([map()]) :: [map()]
  defp dedupe_candidates(candidates) do
    candidates
    |> Enum.reduce(%{}, fn candidate, acc ->
      key = candidate_key(candidate.record)

      Map.update(acc, key, candidate, fn existing ->
        if tier_priority(candidate.tier) >= tier_priority(existing.tier) do
          candidate
        else
          existing
        end
      end)
    end)
    |> Map.values()
  end

  @spec rank_candidates([map()], map(), map(), integer()) :: {:ok, [map()]} | {:error, term()}
  defp rank_candidates(candidates, query_map, retrieval_cfg, now) do
    persona_keys = normalize_tags(map_get(query_map, :persona_keys, []))
    text_filter = map_get(query_map, :text_contains)

    ranked =
      candidates
      |> Enum.map(fn %{record: record, tier: tier} = candidate ->
        heat = heat_component(record)
        persona = persona_component(record, persona_keys)
        semantic = Float.round(0.7 * heat + 0.3 * persona, 4)
        lexical = lexical_component(record, text_filter)
        recency = recency_component(record, now)
        tier_bias = tier_bias(tier)

        final_score =
          Float.round(
            retrieval_cfg.ranking.lexical_weight * lexical +
              retrieval_cfg.ranking.semantic_weight * semantic +
              0.15 * recency + tier_bias,
            4
          )

        features = %{
          lexical: lexical,
          semantic: semantic,
          recency: recency,
          heat: heat,
          persona: persona,
          tier_bias: tier_bias,
          final_score: final_score
        }

        Map.put(candidate, :features, features)
      end)
      |> Enum.sort(fn left, right ->
        cond do
          left.features.final_score > right.features.final_score ->
            true

          left.features.final_score < right.features.final_score ->
            false

          (left.record.observed_at || 0) > (right.record.observed_at || 0) ->
            true

          (left.record.observed_at || 0) < (right.record.observed_at || 0) ->
            false

          true ->
            left.record.id <= right.record.id
        end
      end)

    {:ok, ranked}
  end

  @spec resolve_tier_mode(map(), keyword()) :: atom()
  defp resolve_tier_mode(query_map, runtime_opts) do
    mode =
      map_get(query_map, :tier_mode) ||
        Keyword.get(runtime_opts, :tier_mode) ||
        Keyword.get(runtime_opts, :tier, :short)

    case mode do
      :short -> :short
      :mid -> :mid
      :long -> :long
      :hybrid -> :hybrid
      :all -> :all
      "short" -> :short
      "mid" -> :mid
      "long" -> :long
      "hybrid" -> :hybrid
      "all" -> :all
      _ -> :short
    end
  end

  @spec mode_tiers(atom()) :: [Config.tier()]
  defp mode_tiers(:short), do: [:short]
  defp mode_tiers(:mid), do: [:mid]
  defp mode_tiers(:long), do: [:long]
  defp mode_tiers(:hybrid), do: [:short, :mid, :long]
  defp mode_tiers(:all), do: [:short, :mid, :long]

  @spec tier_priority(Config.tier()) :: integer()
  defp tier_priority(:short), do: 3
  defp tier_priority(:mid), do: 2
  defp tier_priority(:long), do: 1

  @spec tier_bias(Config.tier()) :: number()
  defp tier_bias(:short), do: 0.1
  defp tier_bias(:mid), do: 0.05
  defp tier_bias(:long), do: 0.0

  @spec candidate_key(Jido.Memory.Record.t()) :: {String.t(), String.t()}
  defp candidate_key(record), do: {record.namespace, record.id}

  @spec recency_component(Jido.Memory.Record.t(), integer()) :: number()
  defp recency_component(record, now) do
    age = max(0, now - (record.observed_at || now))
    clamp(1.0 - age / 86_400_000, 0.0, 1.0)
  end

  @spec heat_component(Jido.Memory.Record.t()) :: number()
  defp heat_component(record) do
    case Metadata.from_record(record) do
      {:ok, mem_os} -> clamp(mem_os.heat, 0.0, 1.0)
      _ -> 0.0
    end
  end

  @spec persona_component(Jido.Memory.Record.t(), [String.t()]) :: number()
  defp persona_component(_record, []), do: 0.5

  defp persona_component(record, persona_keys) do
    case Metadata.from_record(record) do
      {:ok, mem_os} ->
        if Enum.any?(mem_os.persona_keys, &(&1 in persona_keys)), do: 1.0, else: 0.0

      _ ->
        0.0
    end
  end

  @spec lexical_component(Jido.Memory.Record.t(), term()) :: number()
  defp lexical_component(_record, nil), do: 1.0

  defp lexical_component(record, text_filter) when is_binary(text_filter) do
    haystack =
      cond do
        is_binary(record.text) and record.text != "" -> record.text
        true -> inspect(record.content)
      end

    if String.contains?(String.downcase(haystack), String.downcase(String.trim(text_filter))),
      do: 1.0,
      else: 0.0
  end

  defp lexical_component(_record, _text_filter), do: 1.0

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value

  @spec remember_short(map() | struct(), map() | keyword(), keyword(), state(), String.t()) ::
          {{:ok, Jido.Memory.Record.t()} | {:error, term()}, state()}
  defp remember_short(target, attrs, runtime_opts, state, trace_id) do
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

      case retry_runtime_remember(
             target,
             attrs_with_ingest,
             remember_opts,
             state,
             :remember,
             trace_id
           ) do
        {{:ok, record}, state1} ->
          updated_state =
            state1
            |> put_turn_counter(context.namespace, normalized.next_turn_index)
            |> enqueue_short_candidate(context.namespace, record.id)
            |> enforce_short_maintenance(target, runtime_opts, context)
            |> maybe_schedule_auto_consolidation(context.namespace, target, runtime_opts)

          {{:ok, record}, updated_state}

        {{:error, reason}, state1} ->
          dead_letter = %{
            at: now,
            trace_id: trace_id,
            operation: :remember,
            target: summarize_target(target),
            attrs: attrs_with_ingest,
            reason: inspect(reason)
          }

          state2 = push_dead_letter(state1, dead_letter)
          {{:error, to_jido_error(reason, :remember)}, state2}
      end
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, :remember)}, state}
    end
  end

  @spec retry_runtime_remember(
          map() | struct(),
          map() | keyword(),
          keyword(),
          state(),
          atom(),
          String.t()
        ) ::
          {{:ok, Jido.Memory.Record.t()} | {:error, term()}, state()}
  defp retry_runtime_remember(target, attrs, opts, state, operation, trace_id) do
    retries = state.policy_cache.manager.retry_attempts
    backoff_ms = state.policy_cache.manager.retry_backoff_ms
    jitter_ms = state.policy_cache.manager.retry_jitter_ms

    do_retry_runtime_remember(
      target,
      attrs,
      opts,
      operation,
      trace_id,
      retries + 1,
      backoff_ms,
      jitter_ms,
      state
    )
  end

  @spec do_retry_runtime_remember(
          map() | struct(),
          map() | keyword(),
          keyword(),
          atom(),
          String.t(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer(),
          state()
        ) :: {{:ok, Jido.Memory.Record.t()} | {:error, term()}, state()}
  defp do_retry_runtime_remember(
         target,
         attrs,
         opts,
         operation,
         trace_id,
         attempts_left,
         backoff_ms,
         jitter_ms,
         state
       ) do
    case MemoryRuntime.remember(target, attrs, opts) do
      {:ok, record} ->
        {{:ok, record}, state}

      {:error, reason} when attempts_left > 1 ->
        if transient_runtime_error?(reason) do
          sleep_ms = backoff_ms + random_jitter(jitter_ms)
          if sleep_ms > 0, do: Process.sleep(sleep_ms)

          state1 = increment_metric(state, :retried)

          do_retry_runtime_remember(
            target,
            attrs,
            opts,
            operation,
            trace_id,
            attempts_left - 1,
            backoff_ms,
            jitter_ms,
            state1
          )
        else
          {{:error, reason}, state}
        end

      {:error, reason} ->
        {{:error, enrich_retry_context(reason, operation, trace_id)}, state}
    end
  end

  @spec transient_runtime_error?(term()) :: boolean()
  defp transient_runtime_error?(%Jido.Error.ExecutionError{details: details})
       when is_map(details) do
    Map.get(details, :code) in [:upstream_error, :upstream_error_list, :runtime_exception]
  end

  defp transient_runtime_error?({:runtime_exception, _exception, _stacktrace}), do: true
  defp transient_runtime_error?({:put_failed, _reason}), do: true
  defp transient_runtime_error?({:query_failed, _reason}), do: true
  defp transient_runtime_error?(_), do: false

  @spec enrich_retry_context(term(), atom(), String.t()) :: term()
  defp enrich_retry_context(%Jido.Error.ExecutionError{} = error, _operation, trace_id) do
    details = Map.put(error.details || %{}, :trace_id, trace_id)
    %{error | details: details}
  end

  defp enrich_retry_context(reason, operation, trace_id) do
    ErrorMapping.from_reason(reason, operation)
    |> Map.update!(:details, &Map.put(&1, :trace_id, trace_id))
  end

  @spec random_jitter(non_neg_integer()) :: non_neg_integer()
  defp random_jitter(0), do: 0
  defp random_jitter(max_value) when max_value > 0, do: :rand.uniform(max_value) - 1

  @spec maybe_schedule_auto_consolidation(state(), String.t(), map() | struct(), keyword()) ::
          state()
  defp maybe_schedule_auto_consolidation(state, namespace, target, runtime_opts) do
    if state.policy_cache.manager.auto_consolidate do
      debounce_ms = state.policy_cache.manager.consolidation_debounce_ms

      if existing = Map.get(state.pending_consolidation, namespace) do
        Process.cancel_timer(existing.timer_ref)
      end

      dispatch_ref = make_ref()

      timer_ref =
        Process.send_after(self(), {:auto_consolidate, namespace, dispatch_ref}, debounce_ms)

      pending =
        Map.put(state.pending_consolidation, namespace, %{
          ref: dispatch_ref,
          timer_ref: timer_ref,
          target: target,
          opts:
            runtime_opts |> Keyword.delete(:operation_sleep_ms) |> Keyword.delete(:call_timeout)
        })

      %{state | pending_consolidation: pending}
    else
      state
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
        phase: 3,
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
    Keyword.put_new(runtime_opts, :app_config, state.policy_cache)
  end

  @spec request_agent_key(map() | struct(), keyword()) :: String.t()
  defp request_agent_key(target, runtime_opts) do
    explicit = Keyword.get(runtime_opts, :agent_id) || Keyword.get(runtime_opts, :agent_key)

    cond do
      is_binary(explicit) and String.trim(explicit) != "" -> String.trim(explicit)
      is_binary(map_get(target, :id)) -> map_get(target, :id)
      is_binary(map_get(runtime_opts, :namespace)) -> "ns:" <> map_get(runtime_opts, :namespace)
      true -> "anonymous"
    end
  end

  @spec normalize_timeout(term(), pos_integer()) :: pos_integer()
  defp normalize_timeout(value, _fallback) when is_integer(value) and value > 0, do: value
  defp normalize_timeout(_value, fallback), do: fallback

  @spec normalize_trace_id(term()) :: String.t()
  defp normalize_trace_id(value) when is_binary(value) and value != "", do: value

  defp normalize_trace_id(_value) do
    "mm-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  @spec normalize_agent_key(term()) :: String.t() | :any
  defp normalize_agent_key(nil), do: :any

  defp normalize_agent_key(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: :any, else: trimmed
  end

  defp normalize_agent_key(_value), do: :any

  @spec summarize_target(term()) :: map()
  defp summarize_target(%{id: id}) when is_binary(id), do: %{id: id}

  defp summarize_target(target) when is_map(target),
    do: Map.take(target, [:id, :agent_id, "id", "agent_id"])

  defp summarize_target(_), do: %{}

  @spec push_dead_letter(state(), map()) :: state()
  defp push_dead_letter(state, entry) do
    limit = state.policy_cache.manager.dead_letter_limit
    dead_letters = [entry | state.dead_letters] |> Enum.take(limit)

    state
    |> Map.put(:dead_letters, dead_letters)
    |> increment_metric(:dead_lettered)
  end

  @spec maybe_reply(state(), GenServer.from() | nil, term()) :: state()
  defp maybe_reply(state, nil, _result), do: state

  defp maybe_reply(state, from, result) do
    GenServer.reply(from, result)
    state
  end

  @spec increment_metric(state(), atom(), non_neg_integer()) :: state()
  defp increment_metric(state, metric, increment \\ 1) do
    %{state | metrics: Map.update(state.metrics, metric, increment, &(&1 + increment))}
  end

  @spec to_query_map(term()) :: {:ok, map()} | {:error, term()}
  defp to_query_map(%Jido.Memory.Query{} = query), do: {:ok, Map.from_struct(query)}
  defp to_query_map(%{} = query), do: {:ok, query}

  defp to_query_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: {:ok, Map.new(list)}, else: {:error, :invalid_query}
  end

  defp to_query_map(_), do: {:error, :invalid_query}

  @spec normalize_integer(term(), integer()) :: integer()
  defp normalize_integer(value, _fallback) when is_integer(value), do: value
  defp normalize_integer(_value, fallback), do: fallback

  @spec normalize_tags(term()) :: [String.t()]
  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_tags(tags) when is_binary(tags), do: [String.trim(tags)]
  defp normalize_tags(_), do: []

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
