defmodule Jido.MemoryOS.FrameworkAdapter.SingleAgent do
  @moduledoc """
  Reference adapter for standard single-agent request/response loops.
  """

  @behaviour Jido.MemoryOS.FrameworkAdapter

  alias Jido.MemoryOS.FrameworkAdapter

  @default_limit 6

  @impl Jido.MemoryOS.FrameworkAdapter
  def pre_turn(target, turn_input, opts \\ []) do
    default_limit = Keyword.get(opts, :default_limit, @default_limit)
    query = FrameworkAdapter.build_query(turn_input, default_limit)
    memory_opts = FrameworkAdapter.memory_opts(opts, turn_input)

    case Jido.MemoryOS.explain_retrieval(target, query, memory_opts) do
      {:ok, explain} ->
        {:ok,
         %{
           query: query,
           context_pack: FrameworkAdapter.map_get(explain, :context_pack, %{}),
           records: FrameworkAdapter.map_get(explain, :records, []),
           retrieval: summarize_retrieval(explain)
         }}

      {:error, reason} ->
        {:error, normalize_error(reason, :pre_turn)}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def post_turn(target, turn_output, opts \\ []) do
    attrs =
      FrameworkAdapter.build_memory_attrs(turn_output,
        class: :episodic,
        kind: :response,
        source: "framework.single_agent",
        default_tags: ["adapter:single_agent", "memory_os:framework"]
      )

    memory_opts = FrameworkAdapter.memory_opts(opts, turn_output)

    case Jido.MemoryOS.remember(target, attrs, memory_opts) do
      {:ok, record} ->
        {:ok, %{record: record, memory_id: record.id}}

      {:error, reason} ->
        {:error, normalize_error(reason, :post_turn)}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def normalize_error(reason, phase), do: FrameworkAdapter.normalize_error(reason, phase)

  @spec summarize_retrieval(map()) :: map()
  defp summarize_retrieval(explain) do
    %{
      result_count: FrameworkAdapter.map_get(explain, :result_count, 0),
      tier_mode: FrameworkAdapter.map_get(explain, :tier_mode),
      planner: FrameworkAdapter.map_get(explain, :planner, %{}),
      semantic_provider: FrameworkAdapter.map_get(explain, :semantic_provider, %{}),
      decision_trace: FrameworkAdapter.map_get(explain, :decision_trace, []),
      selection_rationale: FrameworkAdapter.map_get(explain, :selection_rationale, %{}),
      excluded: FrameworkAdapter.map_get(explain, :excluded, [])
    }
  end
end
