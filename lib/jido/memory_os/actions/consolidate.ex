defmodule Jido.MemoryOS.Actions.Consolidate do
  @moduledoc """
  Action wrapper for `Jido.MemoryOS.consolidate/2`.
  """

  use Jido.Action,
    name: "memory_os_consolidate",
    description: "Run tier consolidation via MemoryOS",
    schema: [
      tier: [type: :any, required: false, doc: "Tier override for consolidation context"],
      namespace: [type: :string, required: false, doc: "Explicit namespace override"],
      correlation_id: [type: :string, required: false, doc: "Trace correlation id"],
      store: [type: :any, required: false, doc: "Store declaration override"],
      store_opts: [type: :any, required: false, doc: "Store options override"],
      server: [type: :any, required: false, doc: "Memory manager server override"],
      config: [type: :any, required: false, doc: "Runtime config overrides"],
      app_config: [type: :any, required: false, doc: "Application config overrides"],
      agent_id: [type: :any, required: false, doc: "Agent id override for namespace resolution"],
      timeout_ms: [type: :any, required: false, doc: "Manager request timeout override"],
      call_timeout: [type: :any, required: false, doc: "GenServer call timeout override"],
      memory_result_key: [
        type: :any,
        required: false,
        doc: "Result key for consolidation summary"
      ]
    ]

  @option_keys [
    :tier,
    :namespace,
    :correlation_id,
    :store,
    :store_opts,
    :server,
    :config,
    :app_config,
    :agent_id,
    :timeout_ms,
    :call_timeout
  ]

  @impl true
  def run(params, context) do
    map_params = normalize_map(params)
    opts = extract_opts(map_params)

    case Jido.MemoryOS.consolidate(context, opts) do
      {:ok, summary} ->
        key = map_get(map_params, :memory_result_key, :memory_consolidation)
        {:ok, %{key => summary}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec extract_opts(map()) :: keyword()
  defp extract_opts(params) do
    Enum.reduce(@option_keys, [], fn key, acc ->
      case map_get(params, key) do
        nil -> acc
        value -> [{key, value} | acc]
      end
    end)
  end

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
