defmodule Jido.MemoryOS.Actions.Remember do
  @moduledoc """
  Action wrapper for `Jido.MemoryOS.remember/3`.
  """

  use Jido.Action,
    name: "memory_os_remember",
    description: "Remember one item via MemoryOS",
    schema: [
      tier: [type: :any, required: false, doc: "Target tier: short|mid|long"],
      namespace: [type: :string, required: false, doc: "Explicit namespace override"],
      correlation_id: [type: :string, required: false, doc: "Trace correlation id"],
      store: [type: :any, required: false, doc: "Store declaration override"],
      store_opts: [type: :any, required: false, doc: "Store options override"],
      mem_os: [type: :any, required: false, doc: "MemoryOS lifecycle metadata overrides"],
      class: [type: :any, required: false, doc: "Memory class"],
      kind: [type: :any, required: false, doc: "Memory kind"],
      text: [type: :any, required: false, doc: "Searchable text"],
      content: [type: :any, required: false, doc: "Structured payload"],
      tags: [type: :any, required: false, doc: "Tag list"],
      source: [type: :any, required: false, doc: "Source string"],
      observed_at: [type: :any, required: false, doc: "Timestamp ms"],
      expires_at: [type: :any, required: false, doc: "Expiration timestamp ms"],
      metadata: [type: :any, required: false, doc: "Metadata map"],
      memory_result_key: [type: :any, required: false, doc: "Result key for record id"]
    ]

  @option_keys [:tier, :namespace, :correlation_id, :store, :store_opts, :mem_os]

  @impl true
  def run(params, context) do
    map_params = normalize_map(params)
    attrs = Map.drop(map_params, @option_keys ++ [:memory_result_key])
    opts = extract_opts(map_params)

    case Jido.MemoryOS.remember(context, attrs, opts) do
      {:ok, record} ->
        key = map_get(map_params, :memory_result_key, :last_memory_id)
        {:ok, %{key => record.id}}

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
