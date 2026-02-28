defmodule Jido.MemoryOS.Actions.Forget do
  @moduledoc """
  Action wrapper for `Jido.MemoryOS.forget/3`.
  """

  use Jido.Action,
    name: "memory_os_forget",
    description: "Forget one record via MemoryOS",
    schema: [
      id: [type: :string, required: true, doc: "Record id"],
      tier: [type: :any, required: false, doc: "Tier strategy override"],
      namespace: [type: :string, required: false, doc: "Explicit namespace override"],
      correlation_id: [type: :string, required: false, doc: "Trace correlation id"],
      store: [type: :any, required: false, doc: "Store declaration override"],
      store_opts: [type: :any, required: false, doc: "Store options override"],
      memory_result_key: [type: :any, required: false, doc: "Result key for delete boolean"]
    ]

  @option_keys [:tier, :namespace, :correlation_id, :store, :store_opts]

  @impl true
  def run(params, context) do
    map_params = normalize_map(params)
    id = map_get(map_params, :id)
    opts = extract_opts(map_params)

    case Jido.MemoryOS.forget(context, id, opts) do
      {:ok, deleted?} ->
        key = map_get(map_params, :memory_result_key, :last_memory_deleted?)
        {:ok, %{key => deleted?}}

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
