defmodule Jido.MemoryOS.FrameworkAdapter.MultiAgent do
  @moduledoc """
  Reference adapter for multi-agent loops with shared memory orchestration.
  """

  @behaviour Jido.MemoryOS.FrameworkAdapter

  alias Jido.MemoryOS.FrameworkAdapter

  @default_limit 6

  @impl Jido.MemoryOS.FrameworkAdapter
  def pre_turn(target, turn_input, opts \\ []) do
    default_limit = Keyword.get(opts, :default_limit, @default_limit)
    allow_partial = Keyword.get(opts, :allow_partial, true)
    query0 = FrameworkAdapter.build_query(turn_input, default_limit)

    participants = build_participants(target, turn_input, opts)

    per_target_limit =
      max(div(max(Map.get(query0, :limit, default_limit), 1), max(length(participants), 1)), 1)

    query = Map.put(query0, :limit, per_target_limit)
    memory_opts = FrameworkAdapter.memory_opts(opts, turn_input)

    results =
      Enum.map(participants, fn participant ->
        {participant, Jido.MemoryOS.explain_retrieval(participant.target, query, memory_opts)}
      end)

    successes =
      for {participant, {:ok, explain}} <- results do
        {participant, explain}
      end

    errors =
      for {participant, {:error, reason}} <- results do
        %{agent_id: participant.id, error: normalize_error(reason, :pre_turn)}
      end

    cond do
      successes == [] ->
        {:error, first_error(errors, :pre_turn)}

      errors != [] and not allow_partial ->
        {:error, hd(errors).error}

      true ->
        {:ok,
         %{
           query: query,
           context_pack: merge_context_packs(successes),
           records_by_agent: records_by_agent(successes),
           retrieval: %{
             per_target_limit: per_target_limit,
             participants: Enum.map(participants, & &1.id),
             participant_results: participant_results(successes),
             partial_errors: errors
           }
         }}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def post_turn(target, turn_output, opts \\ []) do
    allow_partial = Keyword.get(opts, :allow_partial, true)
    broadcast_shared = Keyword.get(opts, :broadcast_shared, true)

    participants =
      build_participants(target, turn_output, opts)
      |> maybe_scope_post_targets(broadcast_shared)

    attrs =
      FrameworkAdapter.build_memory_attrs(turn_output,
        class: :episodic,
        kind: :response,
        source: "framework.multi_agent",
        default_tags: ["adapter:multi_agent", "memory_os:framework"]
      )

    memory_opts = FrameworkAdapter.memory_opts(opts, turn_output)

    writes =
      Enum.map(participants, fn participant ->
        remember_for_participant(participant, attrs, memory_opts)
      end)

    successes =
      for {:ok, payload} <- writes do
        payload
      end

    errors =
      for {:error, payload} <- writes do
        payload
      end

    cond do
      successes == [] ->
        {:error, first_error(errors, :post_turn)}

      errors != [] and not allow_partial ->
        {:error, hd(errors).error}

      true ->
        {:ok, %{written: successes, failures: errors}}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def normalize_error(reason, phase), do: FrameworkAdapter.normalize_error(reason, phase)

  @spec maybe_scope_post_targets([map()], boolean()) :: [map()]
  defp maybe_scope_post_targets(participants, true), do: participants

  defp maybe_scope_post_targets(participants, false) do
    Enum.filter(participants, &(&1.role == :primary))
  end

  @spec build_participants(map() | struct(), map() | keyword(), keyword()) :: [map()]
  defp build_participants(target, turn_input, opts) do
    primary = normalize_target(target, :primary)

    shared_targets =
      Keyword.get(opts, :shared_targets) ||
        FrameworkAdapter.map_get(FrameworkAdapter.normalize_map(turn_input), :shared_targets, [])

    shared =
      shared_targets
      |> normalize_shared_targets()
      |> Enum.reject(&(&1.id == primary.id))
      |> Enum.uniq_by(& &1.id)

    [primary | shared]
  end

  @spec normalize_target(map() | struct() | term(), :primary | :shared) :: map()
  defp normalize_target(target, role) when is_map(target) do
    id =
      FrameworkAdapter.target_id(target) ||
        "agent-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

    %{id: id, target: target, role: role}
  end

  defp normalize_target(id, role) when is_binary(id) do
    %{id: id, target: %{id: id}, role: role}
  end

  defp normalize_target(id, role) when is_atom(id), do: normalize_target(Atom.to_string(id), role)

  defp normalize_target({id, %{} = target}, role) when is_binary(id) do
    %{id: id, target: Map.put_new(target, :id, id), role: role}
  end

  defp normalize_target(other, role) do
    normalize_target(inspect(other), role)
  end

  @spec normalize_shared_targets(term()) :: [map()]
  defp normalize_shared_targets(targets) when is_list(targets) do
    Enum.map(targets, &normalize_target(&1, :shared))
  end

  defp normalize_shared_targets(nil), do: []
  defp normalize_shared_targets(target), do: [normalize_target(target, :shared)]

  @spec remember_for_participant(map(), map(), keyword()) ::
          {:ok, map()} | {:error, %{agent_id: String.t(), role: atom(), error: term()}}
  defp remember_for_participant(participant, attrs, memory_opts) do
    role_tag = "scope:" <> Atom.to_string(participant.role)

    metadata =
      attrs
      |> FrameworkAdapter.map_get(:metadata, %{})
      |> FrameworkAdapter.normalize_map()
      |> Map.put("target_role", Atom.to_string(participant.role))
      |> Map.put("target_id", participant.id)

    participant_attrs =
      attrs
      |> Map.put(
        :tags,
        Enum.uniq(
          FrameworkAdapter.normalize_tags(FrameworkAdapter.map_get(attrs, :tags, [])) ++
            [role_tag]
        )
      )
      |> Map.put(:metadata, metadata)

    case Jido.MemoryOS.remember(participant.target, participant_attrs, memory_opts) do
      {:ok, record} ->
        {:ok,
         %{agent_id: participant.id, role: participant.role, record: record, memory_id: record.id}}

      {:error, reason} ->
        {:error,
         %{
           agent_id: participant.id,
           role: participant.role,
           error: normalize_error(reason, :post_turn)
         }}
    end
  end

  @spec merge_context_packs([{map(), map()}]) :: map()
  defp merge_context_packs(successes) do
    Enum.reduce(
      successes,
      %{token_budget: 0, tokens_used: 0, truncated: false, groups: []},
      fn {participant, explain}, acc ->
        context_pack = FrameworkAdapter.map_get(explain, :context_pack, %{})
        groups = FrameworkAdapter.map_get(context_pack, :groups, [])

        enriched_groups =
          Enum.map(groups, fn group ->
            group
            |> Map.put(:agent_id, participant.id)
            |> Map.put_new(:label, "agent:" <> participant.id)
          end)

        %{
          token_budget:
            acc.token_budget + FrameworkAdapter.map_get(context_pack, :token_budget, 0),
          tokens_used: acc.tokens_used + FrameworkAdapter.map_get(context_pack, :tokens_used, 0),
          truncated: acc.truncated or FrameworkAdapter.map_get(context_pack, :truncated, false),
          groups: acc.groups ++ enriched_groups
        }
      end
    )
  end

  @spec records_by_agent([{map(), map()}]) :: map()
  defp records_by_agent(successes) do
    Map.new(successes, fn {participant, explain} ->
      {participant.id, FrameworkAdapter.map_get(explain, :records, [])}
    end)
  end

  @spec participant_results([{map(), map()}]) :: [map()]
  defp participant_results(successes) do
    Enum.map(successes, fn {participant, explain} ->
      %{
        agent_id: participant.id,
        role: participant.role,
        result_count: FrameworkAdapter.map_get(explain, :result_count, 0),
        decision_trace: FrameworkAdapter.map_get(explain, :decision_trace, [])
      }
    end)
  end

  @spec first_error([map()], atom()) :: term()
  defp first_error([], phase), do: normalize_error(:no_results, phase)
  defp first_error([%{error: error} | _rest], _phase), do: error
  defp first_error(_errors, phase), do: normalize_error(:unknown_error, phase)
end
