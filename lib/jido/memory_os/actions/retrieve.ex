defmodule Jido.MemoryOS.Actions.Retrieve do
  @moduledoc """
  Action wrapper for `Jido.MemoryOS.retrieve/3`.
  """

  use Jido.Action,
    name: "memory_os_retrieve",
    description: "Retrieve records via MemoryOS",
    schema: [
      tier: [type: :any, required: false, doc: "Tier strategy override"],
      namespace: [type: :string, required: false, doc: "Explicit namespace override"],
      correlation_id: [type: :string, required: false, doc: "Trace correlation id"],
      store: [type: :any, required: false, doc: "Store declaration override"],
      store_opts: [type: :any, required: false, doc: "Store options override"],
      limit: [type: :any, required: false, doc: "Maximum result count"],
      classes: [type: :any, required: false, doc: "Class filters"],
      kinds: [type: :any, required: false, doc: "Kind filters"],
      tags_any: [type: :any, required: false, doc: "Tag OR filter"],
      tags_all: [type: :any, required: false, doc: "Tag AND filter"],
      text_contains: [type: :any, required: false, doc: "Text filter"],
      since: [type: :any, required: false, doc: "Start timestamp filter"],
      until: [type: :any, required: false, doc: "End timestamp filter"],
      order: [type: :any, required: false, doc: "Sort order"],
      memory_result_key: [type: :any, required: false, doc: "Result key for records"]
    ]

  @option_keys [:tier, :namespace, :correlation_id, :store, :store_opts]

  @impl true
  def run(params, context) do
    map_params = normalize_map(params)
    query = Map.drop(map_params, @option_keys ++ [:memory_result_key])
    opts = extract_opts(map_params)

    case Jido.MemoryOS.retrieve(context, query, opts) do
      {:ok, records} ->
        key = map_get(map_params, :memory_result_key, :memory_results)
        {:ok, %{key => records}}

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
