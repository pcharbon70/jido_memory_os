defmodule Jido.MemoryOS.MemoryManager do
  @moduledoc """
  Phase 3 control-plane orchestrator for ingestion, retrieval, consolidation,
  and maintenance operations.
  """

  use GenServer

  alias Jido.MemoryOS.Adapter.MemoryRuntime

  alias Jido.MemoryOS.{
    AccessPolicy,
    ApprovalToken,
    Config,
    DataSafety,
    ErrorMapping,
    Lifecycle,
    Metadata,
    Query
  }

  alias Jido.MemoryOS.Retrieval.{Candidate, ContextPack, Planner, Ranker}

  @type candidate :: {String.t(), String.t()}
  @type operation ::
          :remember
          | :retrieve
          | :explain_retrieval
          | :forget
          | :prune
          | :consolidate
          | :policy_update

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
          approval_tokens: %{optional(String.t()) => map()},
          audit_log: [map()],
          audit_seq: non_neg_integer(),
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
  Updates access policy at runtime (approval-gated when configured).
  """
  @spec update_policy(map() | struct(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def update_policy(target, policy, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    call_timeout = Keyword.get(runtime_opts, :call_timeout, :infinity)
    GenServer.call(server, {:update_policy, target, policy, runtime_opts}, call_timeout)
  end

  @doc """
  Issues approval token for gated operations.
  """
  @spec issue_approval_token(GenServer.server(), keyword()) :: {:ok, map()} | {:error, term()}
  def issue_approval_token(server \\ __MODULE__, opts \\ []) do
    GenServer.call(server, {:issue_approval_token, opts})
  end

  @doc """
  Returns immutable audit events (newest first).
  """
  @spec audit_events(GenServer.server(), keyword()) :: {:ok, [map()]}
  def audit_events(server \\ __MODULE__, opts \\ []) do
    GenServer.call(server, {:audit_events, opts})
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
           approval_tokens: %{},
           audit_log: [],
           audit_seq: 0,
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
  def handle_call({:update_policy, target, policy, runtime_opts}, from, state) do
    enqueue_reply_request(:policy_update, target, policy, runtime_opts, from, state)
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
      pending_agents: map_size(state.queue_by_agent),
      approvals_active: map_size(state.approval_tokens),
      audit_events: length(state.audit_log)
    }

    {:reply, {:ok, Map.merge(state.metrics, queue_metrics)}, state}
  end

  @impl true
  def handle_call({:audit_events, opts}, _from, state) do
    limit = max(1, normalize_integer(Keyword.get(opts, :limit), 200))
    {:reply, {:ok, Enum.take(state.audit_log, limit)}, state}
  end

  @impl true
  def handle_call({:issue_approval_token, opts}, _from, state) do
    approvals_cfg = approval_config(state)
    actor_id = normalize_optional_string(Keyword.get(opts, :actor_id))
    actions = normalize_atom_list(Keyword.get(opts, :actions))
    issue_opts = [actor_id: actor_id, reason: Keyword.get(opts, :reason)]

    issue_opts =
      issue_opts
      |> maybe_put_kw(:actions, if(actions == [], do: nil, else: actions))
      |> maybe_put_kw(:ttl_ms, Keyword.get(opts, :ttl_ms))
      |> maybe_put_kw(:one_time, Keyword.get(opts, :one_time))

    defaults = [
      ttl_ms: map_get(approvals_cfg, :ttl_ms, 300_000),
      max_tokens: map_get(approvals_cfg, :max_tokens, 500),
      one_time: map_get(approvals_cfg, :one_time, true),
      actions: map_get(approvals_cfg, :required_actions, [])
    ]

    {:ok, entry, tokens} = ApprovalToken.issue(state.approval_tokens, issue_opts, defaults)

    state1 =
      state
      |> Map.put(:approval_tokens, tokens)
      |> append_audit_event(%{
        category: :approval,
        outcome: :issued,
        actor_id: actor_id,
        action: :issue_approval_token,
        metadata: %{
          token: entry.token,
          actions: entry.actions,
          expires_at: entry.expires_at,
          reason: entry.reason
        }
      })

    {:reply, {:ok, entry}, state1}
  end

  @impl true
  def handle_call({:cancel_pending, opts}, _from, state) do
    agent_filter =
      normalize_agent_key(Keyword.get(opts, :agent_id) || Keyword.get(opts, :agent_key))

    op_filter =
      case Keyword.get(opts, :operation) do
        nil ->
          :any

        op
        when op in [
               :remember,
               :retrieve,
               :explain_retrieval,
               :forget,
               :prune,
               :consolidate,
               :policy_update
             ] ->
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
        policy_context = policy_context(request)
        before_pointer = mutation_before_pointer(request, state)

        with {:ok, state1} <- enforce_policy(request, policy_context, state),
             {:ok, state2} <- enforce_approval_if_required(request, policy_context, state1),
             {result, state3} <- dispatch_request(request, state2) do
          state4 = audit_operation_result(request, policy_context, result, before_pointer, state3)
          {result, state4}
        else
          {:error, reason, state1} ->
            result = {:error, to_jido_error(reason, request.op)}

            {result,
             audit_operation_result(request, policy_context, result, before_pointer, state1)}
        end
      end
    end
  end

  @spec dispatch_request(request(), state()) :: {term(), state()}
  defp dispatch_request(request, state) do
    case request.op do
      :remember -> remember_request(request, state)
      :retrieve -> retrieve_request(request, state, false)
      :explain_retrieval -> retrieve_request(request, state, true)
      :forget -> forget_request(request, state)
      :prune -> prune_request(request, state)
      :consolidate -> consolidate_request(request, state)
      :policy_update -> update_policy_request(request, state)
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

  @spec policy_context(request()) :: AccessPolicy.context()
  defp policy_context(request) do
    AccessPolicy.context_from_request(
      request.op,
      request.target,
      request.runtime_opts,
      request.trace_id
    )
  end

  @spec enforce_policy(request(), AccessPolicy.context(), state()) ::
          {:ok, state()} | {:error, term(), state()}
  defp enforce_policy(request, policy_context, state) do
    decision =
      AccessPolicy.evaluate(policy_context, map_get(state.policy_cache, :governance, %{}).policy)

    state1 =
      append_audit_event(state, %{
        category: :access,
        action: request.op,
        outcome: if(decision.allowed?, do: :allow, else: :deny),
        actor_id: policy_context.actor_id,
        actor_group: policy_context.actor_group,
        actor_role: policy_context.actor_role,
        target_agent_id: policy_context.target_agent_id,
        target_group: policy_context.target_group,
        tier: policy_context.tier,
        trace_id: policy_context.trace_id,
        metadata: %{
          reason: decision.reason,
          effect: decision.effect,
          matched_rule: decision.matched_rule
        }
      })

    if decision.allowed? do
      {:ok, state1}
    else
      {:error, {:access_denied, decision}, state1}
    end
  end

  @spec enforce_approval_if_required(request(), AccessPolicy.context(), state()) ::
          {:ok, state()} | {:error, term(), state()}
  defp enforce_approval_if_required(request, policy_context, state) do
    case approval_action(request) do
      nil ->
        {:ok, state}

      required_action ->
        token = Keyword.get(request.runtime_opts, :approval_token)

        case ApprovalToken.validate(
               state.approval_tokens,
               token,
               required_action,
               policy_context.actor_id
             ) do
          {:ok, entry, tokens} ->
            state1 =
              state
              |> Map.put(:approval_tokens, tokens)
              |> append_audit_event(%{
                category: :approval,
                action: request.op,
                outcome: :approved,
                actor_id: policy_context.actor_id,
                target_agent_id: policy_context.target_agent_id,
                tier: policy_context.tier,
                trace_id: policy_context.trace_id,
                metadata: %{
                  required_action: required_action,
                  token: entry.token,
                  expires_at: entry.expires_at
                }
              })

            {:ok, state1}

          {:error, reason, tokens} ->
            state1 =
              state
              |> Map.put(:approval_tokens, tokens)
              |> append_audit_event(%{
                category: :approval,
                action: request.op,
                outcome: :denied,
                actor_id: policy_context.actor_id,
                target_agent_id: policy_context.target_agent_id,
                tier: policy_context.tier,
                trace_id: policy_context.trace_id,
                metadata: %{required_action: required_action, reason: reason}
              })

            {:error, reason, state1}
        end
    end
  end

  @spec approval_action(request()) :: atom() | nil
  defp approval_action(request) do
    required_actions = approval_required_actions(request.runtime_opts)

    cond do
      :policy_update in required_actions and request.op == :policy_update ->
        :policy_update

      :forget in required_actions and request.op == :forget ->
        :forget

      :overwrite in required_actions and request.op == :remember and
          has_explicit_id?(request.payload) ->
        :overwrite

      true ->
        nil
    end
  end

  @spec has_explicit_id?(term()) :: boolean()
  defp has_explicit_id?(payload) do
    payload
    |> normalize_map()
    |> map_get(:id)
    |> case do
      value when is_binary(value) and value != "" -> true
      _ -> false
    end
  end

  @spec approval_required_actions(keyword()) :: [atom()]
  defp approval_required_actions(runtime_opts) do
    approvals =
      runtime_opts
      |> Keyword.get(:app_config, %{})
      |> map_get(:governance, %{})
      |> map_get(:approvals, %{})

    if map_get(approvals, :enabled, true) do
      approvals
      |> map_get(:required_actions, [])
      |> normalize_atom_list()
    else
      []
    end
  end

  @spec mutation_before_pointer(request(), state()) :: map() | nil
  defp mutation_before_pointer(request, state) do
    case request.op do
      :remember ->
        payload = normalize_map(request.payload)

        case map_get(payload, :id) do
          id when is_binary(id) and id != "" -> %{memory_id: id}
          _ -> nil
        end

      :forget ->
        %{memory_id: request.payload}

      :policy_update ->
        policy = map_get(state.policy_cache, :governance, %{}) |> map_get(:policy, %{})
        %{policy_hash: policy_hash(policy)}

      _ ->
        nil
    end
  end

  @spec mutation_after_pointer(request(), term()) :: map() | nil
  defp mutation_after_pointer(request, result) do
    case {request.op, result} do
      {:remember, {:ok, %Jido.Memory.Record{} = record}} ->
        %{memory_id: record.id, namespace: record.namespace}

      {:forget, {:ok, deleted?}} ->
        %{memory_id: request.payload, deleted?: deleted?}

      {:policy_update, {:ok, policy}} ->
        %{policy_hash: policy_hash(policy)}

      {:explain_retrieval, {:ok, explain}} when is_map(explain) ->
        %{
          result_count: map_get(explain, :result_count, 0),
          explanation_hash: DataSafety.explanation_hash(explain)
        }

      _ ->
        nil
    end
  end

  @spec audit_operation_result(request(), AccessPolicy.context(), term(), map() | nil, state()) ::
          state()
  defp audit_operation_result(request, policy_context, result, before_pointer, state) do
    after_pointer = mutation_after_pointer(request, result)

    {outcome, error_code} =
      case result do
        {:ok, _} -> {:ok, nil}
        {:error, %{} = err} -> {:error, error_code(err)}
        {:error, reason} -> {:error, reason}
        _ -> {:ok, nil}
      end

    metadata =
      %{
        correlation_id: policy_context.correlation_id,
        before_pointer: before_pointer,
        after_pointer: after_pointer
      }
      |> maybe_put_map(:error_code, error_code)
      |> maybe_put_map(:result_shape, result_shape(result))

    append_audit_event(state, %{
      category: :operation,
      action: request.op,
      outcome: outcome,
      actor_id: policy_context.actor_id,
      actor_group: policy_context.actor_group,
      actor_role: policy_context.actor_role,
      target_agent_id: policy_context.target_agent_id,
      target_group: policy_context.target_group,
      tier: policy_context.tier,
      trace_id: policy_context.trace_id,
      metadata: metadata
    })
  end

  @spec result_shape(term()) :: atom()
  defp result_shape({:ok, _}), do: :ok
  defp result_shape({:error, _}), do: :error
  defp result_shape(_), do: :unknown

  @spec remember_request(request(), state()) :: {term(), state()}
  defp remember_request(request, state) do
    runtime_opts = request.runtime_opts

    with {:ok, tier} <- normalize_tier(Keyword.get(runtime_opts, :tier, :short)),
         {:ok, attrs} <-
           DataSafety.sanitize_attrs(
             normalize_map(request.payload),
             tier,
             state.policy_cache,
             System.system_time(:millisecond)
           ) do
      case tier do
        :short ->
          {result, state1} =
            remember_short(request.target, attrs, runtime_opts, state, request.trace_id)

          {result, state1}

        _other ->
          result =
            retry_runtime_remember(
              request.target,
              attrs,
              runtime_opts,
              state,
              :remember,
              request.trace_id
            )

          case result do
            {{:ok, record}, state1} -> {{:ok, record}, state1}
            {{:error, reason}, state1} -> {{:error, to_jido_error(reason, :remember)}, state1}
          end
      end
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, :remember)}, state}
    end
  end

  @spec retrieve_request(request(), state(), boolean()) :: {term(), state()}
  defp retrieve_request(request, state, explain?) do
    policy_ctx = policy_context(request)

    masking_mode =
      AccessPolicy.masking_mode(policy_ctx, map_get(state.policy_cache, :governance, %{}))

    with {:ok, query} <-
           Query.new(request.payload,
             default_limit: state.policy_cache.retrieval.limit,
             tier_mode: Keyword.get(request.runtime_opts, :tier_mode),
             tier: Keyword.get(request.runtime_opts, :tier)
           ),
         query <- maybe_enable_explain_mode(query, explain?),
         {:ok, retrieval} <-
           run_retrieval_pipeline(request.target, query, request.runtime_opts, state) do
      masked_records = DataSafety.mask_records(retrieval.records, masking_mode)

      if explain? do
        explain_payload = %{
          query: Map.from_struct(query),
          result_count: length(masked_records),
          retrieval: state.policy_cache.retrieval,
          lifecycle: state.policy_cache.lifecycle,
          manager: state.policy_cache.manager,
          tier_mode: retrieval.tier_mode,
          planner: retrieval.plan,
          queue: %{depth: state.queue_depth, pending_agents: map_size(state.queue_by_agent)},
          records: Enum.map(masked_records, &record_debug/1),
          scored_candidates: retrieval.scored_candidates,
          excluded: retrieval.excluded,
          context_pack: retrieval.context_pack,
          semantic_provider: retrieval.semantic,
          decision_trace: retrieval.decision_trace,
          selection_rationale: retrieval.selection_rationale,
          recent_conflicts: state.last_conflicts
        }

        {{:ok, DataSafety.mask_explain_payload(explain_payload, masking_mode)}, state}
      else
        {{:ok, masked_records}, state}
      end
    else
      {:error, reason} ->
        {{:error, to_jido_error(reason, if(explain?, do: :explain_retrieval, else: :retrieve))},
         state}
    end
  end

  @spec maybe_enable_explain_mode(Query.t(), boolean()) :: Query.t()
  defp maybe_enable_explain_mode(query, false), do: query

  defp maybe_enable_explain_mode(%Query{} = query, true) do
    %Query{query | include_excluded: true, debug: true}
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

  @spec update_policy_request(request(), state()) :: {term(), state()}
  defp update_policy_request(request, state) do
    governance = map_get(state.policy_cache, :governance, %{})
    next_policy = AccessPolicy.normalize_policy(request.payload)
    next_governance = Map.put(governance, :policy, next_policy)
    next_policy_cache = Map.put(state.policy_cache, :governance, next_governance)

    {{:ok, next_policy}, %{state | policy_cache: next_policy_cache}}
  end

  @spec run_retrieval_pipeline(map() | struct(), Query.t(), keyword(), state()) ::
          {:ok, map()} | {:error, term()}
  defp run_retrieval_pipeline(target, query, runtime_opts, state) do
    now = System.system_time(:millisecond)

    with {:ok, plan0} <- Planner.plan(query, state.policy_cache.retrieval),
         {:ok, primary_candidates} <-
           fetch_tier_candidates(
             target,
             query,
             runtime_opts,
             plan0.primary_tiers,
             plan0.fanout,
             now
           ),
         plan <- Planner.apply_sparse_fallback(plan0, length(primary_candidates), query.limit),
         {:ok, fallback_candidates} <-
           fetch_additional_candidates(
             target,
             query,
             runtime_opts,
             plan0.primary_tiers,
             plan,
             now
           ),
         candidates <- dedupe_candidates(primary_candidates ++ fallback_candidates),
         {:ok, ranking} <- Ranker.rank(query, candidates, state.policy_cache.retrieval) do
      selected = Enum.take(ranking.ranked, query.limit)
      selected_keys = MapSet.new(Enum.map(selected, & &1.candidate.key))
      context_pack = ContextPack.build(query, selected)

      excluded =
        if query.include_excluded do
          ranking.ranked
          |> Enum.reject(&MapSet.member?(selected_keys, &1.candidate.key))
          |> Enum.map(fn ranked_candidate ->
            %{
              id: ranked_candidate.record.id,
              namespace: ranked_candidate.record.namespace,
              tier: ranked_candidate.tier,
              reason: :below_rank_limit,
              final_score: ranked_candidate.features.final_score
            }
          end)
        else
          []
        end

      scored_candidates =
        Enum.map(selected, fn ranked_candidate ->
          %{
            id: ranked_candidate.record.id,
            namespace: ranked_candidate.record.namespace,
            tier: ranked_candidate.tier,
            features: ranked_candidate.features,
            include_reason: include_reason(ranked_candidate, query)
          }
        end)

      decision_trace = [
        %{
          stage: :planner,
          mode: plan.mode,
          primary_tiers: plan0.primary_tiers,
          fallback_tiers: plan0.fallback_tiers,
          fanout: plan.fanout
        },
        %{
          stage: :fetch,
          primary_candidates: length(primary_candidates),
          fallback_candidates: length(fallback_candidates),
          total_candidates: length(candidates)
        },
        %{
          stage: :rank,
          provider: inspect(ranking.semantic.provider),
          degraded?: ranking.semantic.degraded?,
          degradation_reason: ranking.semantic.reason
        },
        %{
          stage: :context_pack,
          tokens_used: context_pack.tokens_used,
          token_budget: context_pack.token_budget,
          truncated: context_pack.truncated
        }
      ]

      {:ok,
       %{
         records: Enum.map(selected, & &1.record),
         tier_mode: query.tier_mode,
         plan: plan,
         semantic: ranking.semantic,
         context_pack: context_pack,
         scored_candidates: scored_candidates,
         excluded: excluded,
         decision_trace: decision_trace,
         selection_rationale: %{
           tie_breaker: "final_score desc, observed_at desc, id asc",
           weights: %{
             lexical: state.policy_cache.retrieval.ranking.lexical_weight,
             semantic: state.policy_cache.retrieval.ranking.semantic_weight,
             recency_bonus: 0.12,
             heat_bonus: 0.08,
             persona_bonus: 0.06,
             topic_bonus: 0.04,
             tier_bias: "short=0.1, mid=0.05, long=0.0"
           }
         }
       }}
    end
  end

  @spec fetch_tier_candidates(
          map() | struct(),
          Query.t(),
          keyword(),
          [Config.tier()],
          map(),
          integer()
        ) :: {:ok, [Candidate.t()]} | {:error, term()}
  defp fetch_tier_candidates(target, query, runtime_opts, tiers, fanout, now) do
    tiers
    |> Enum.reduce_while({:ok, []}, fn tier, {:ok, acc} ->
      tier_opts = Keyword.put(runtime_opts, :tier, tier)
      tier_limit = Map.get(fanout, tier, query.limit)
      query_filters = Query.to_runtime_filters(query, tier_limit)

      case MemoryRuntime.recall(target, query_filters, tier_opts) do
        {:ok, records} ->
          normalized = Enum.map(records, &Candidate.from_record(&1, tier, now))
          {:cont, {:ok, normalized ++ acc}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, candidates} -> {:ok, candidates}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch_additional_candidates(
          map() | struct(),
          Query.t(),
          keyword(),
          [Config.tier()],
          map(),
          integer()
        ) :: {:ok, [Candidate.t()]} | {:error, term()}
  defp fetch_additional_candidates(target, query, runtime_opts, initial_tiers, plan, now) do
    additional_tiers = plan.primary_tiers -- initial_tiers

    if additional_tiers == [] do
      {:ok, []}
    else
      fetch_tier_candidates(target, query, runtime_opts, additional_tiers, plan.fanout, now)
    end
  end

  @spec dedupe_candidates([Candidate.t()]) :: [Candidate.t()]
  defp dedupe_candidates(candidates) do
    candidates
    |> Enum.reduce(%{}, fn candidate, acc ->
      Map.update(acc, candidate.key, candidate, fn existing ->
        if tier_priority(candidate.tier) >= tier_priority(existing.tier) do
          candidate
        else
          existing
        end
      end)
    end)
    |> Map.values()
  end

  @spec include_reason(map(), Query.t()) :: map()
  defp include_reason(ranked_candidate, query) do
    candidate = ranked_candidate.candidate

    %{
      selected: true,
      matched_persona:
        query.persona_keys == [] or Enum.any?(candidate.persona_keys, &(&1 in query.persona_keys)),
      matched_topic:
        query.topic_keys == [] or Enum.any?(candidate.topic_keys, &(&1 in query.topic_keys)),
      policy_outcome: :ranked
    }
  end

  @spec tier_priority(Config.tier()) :: integer()
  defp tier_priority(:short), do: 3
  defp tier_priority(:mid), do: 2
  defp tier_priority(:long), do: 1

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
    namespace = Keyword.get(runtime_opts, :namespace)

    cond do
      is_binary(explicit) and String.trim(explicit) != "" -> String.trim(explicit)
      is_binary(map_get(target, :id)) -> map_get(target, :id)
      is_binary(namespace) and String.trim(namespace) != "" -> "ns:" <> namespace
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

  @spec normalize_integer(term(), integer()) :: integer()
  defp normalize_integer(value, _fallback) when is_integer(value), do: value
  defp normalize_integer(_value, fallback), do: fallback

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil)

  defp map_get(map, key, default) when is_map(map) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp map_get(list, key, default) when is_list(list) do
    if Keyword.keyword?(list),
      do: Keyword.get(list, key, Keyword.get(list, Atom.to_string(key), default)),
      else: default
  end

  defp map_get(_, _key, default), do: default

  @spec approval_config(state()) :: map()
  defp approval_config(state) do
    state
    |> map_get(:policy_cache, %{})
    |> map_get(:governance, %{})
    |> map_get(:approvals, %{})
  end

  @spec audit_config(state()) :: map()
  defp audit_config(state) do
    state
    |> map_get(:policy_cache, %{})
    |> map_get(:governance, %{})
    |> map_get(:audit, %{})
  end

  @spec append_audit_event(state(), map()) :: state()
  defp append_audit_event(state, event) do
    cfg = audit_config(state)

    if map_get(cfg, :enabled, true) do
      seq = state.audit_seq + 1
      max_events = max(1, normalize_integer(map_get(cfg, :max_events), 2_000))

      entry =
        event
        |> normalize_map()
        |> Map.put_new(:at, System.system_time(:millisecond))
        |> Map.put_new(:seq, seq)

      %{state | audit_seq: seq, audit_log: [entry | state.audit_log] |> Enum.take(max_events)}
    else
      state
    end
  end

  @spec normalize_optional_string(term()) :: String.t() | nil
  defp normalize_optional_string(nil), do: nil

  defp normalize_optional_string(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_optional_string(value), do: value |> to_string() |> normalize_optional_string()

  @spec normalize_atom_list(term()) :: [atom()]
  defp normalize_atom_list(list) when is_list(list) do
    list
    |> Enum.map(&normalize_atom/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_atom_list(value), do: normalize_atom_list([value])

  @spec normalize_atom(term()) :: atom() | nil
  defp normalize_atom(value) when is_atom(value) do
    if value in [:forget, :overwrite, :policy_update], do: value, else: nil
  end

  defp normalize_atom(value) when is_binary(value) do
    trimmed = String.trim(value)
    allowed = %{"forget" => :forget, "overwrite" => :overwrite, "policy_update" => :policy_update}

    if trimmed == "" do
      nil
    else
      Map.get(allowed, trimmed)
    end
  end

  defp normalize_atom(_value), do: nil

  @spec maybe_put_kw(keyword(), atom(), term()) :: keyword()
  defp maybe_put_kw(keyword, _key, nil), do: keyword
  defp maybe_put_kw(keyword, key, value), do: Keyword.put(keyword, key, value)

  @spec maybe_put_map(map(), atom(), term()) :: map()
  defp maybe_put_map(map, _key, nil), do: map
  defp maybe_put_map(map, key, value), do: Map.put(map, key, value)

  @spec policy_hash(term()) :: String.t()
  defp policy_hash(policy) do
    policy
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec error_code(map()) :: term()
  defp error_code(err) do
    err
    |> Map.get(:details, %{})
    |> map_get(:code)
  end
end
