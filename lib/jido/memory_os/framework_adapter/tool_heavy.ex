defmodule Jido.MemoryOS.FrameworkAdapter.ToolHeavy do
  @moduledoc """
  Reference adapter for tool-heavy loops with explicit tool trace capture.
  """

  @behaviour Jido.MemoryOS.FrameworkAdapter

  alias Jido.MemoryOS.FrameworkAdapter

  @default_limit 8

  @impl Jido.MemoryOS.FrameworkAdapter
  def pre_turn(target, turn_input, opts \\ []) do
    default_limit = Keyword.get(opts, :default_limit, @default_limit)
    query0 = FrameworkAdapter.build_query(turn_input, default_limit)
    tool_names = tool_names(turn_input)
    tool_tags = Enum.map(tool_names, &("tool:" <> sanitize_tag(&1)))

    query =
      query0
      |> Map.update(:tags_any, tool_tags, fn tags ->
        FrameworkAdapter.normalize_tags(tags) |> Kernel.++(tool_tags) |> Enum.uniq()
      end)
      |> maybe_put_empty_tags_any()

    memory_opts = FrameworkAdapter.memory_opts(opts, turn_input)

    case Jido.MemoryOS.explain_retrieval(target, query, memory_opts) do
      {:ok, explain} ->
        {:ok,
         %{
           query: query,
           context_pack: FrameworkAdapter.map_get(explain, :context_pack, %{}),
           records: FrameworkAdapter.map_get(explain, :records, []),
           retrieval: %{
             result_count: FrameworkAdapter.map_get(explain, :result_count, 0),
             decision_trace: FrameworkAdapter.map_get(explain, :decision_trace, []),
             tool_tags: tool_tags,
             semantic_provider: FrameworkAdapter.map_get(explain, :semantic_provider, %{})
           }
         }}

      {:error, reason} ->
        {:error, normalize_error(reason, :pre_turn)}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def post_turn(target, turn_output, opts \\ []) do
    allow_partial = Keyword.get(opts, :allow_partial, true)
    memory_opts = FrameworkAdapter.memory_opts(opts, turn_output)

    assistant_attrs =
      FrameworkAdapter.build_memory_attrs(turn_output,
        class: :episodic,
        kind: :response,
        source: "framework.tool_heavy",
        default_tags: ["adapter:tool_heavy", "memory_os:framework"]
      )

    assistant_result =
      case Jido.MemoryOS.remember(target, assistant_attrs, memory_opts) do
        {:ok, record} ->
          {:ok, %{record: record, memory_id: record.id}}

        {:error, reason} ->
          {:error, %{error: normalize_error(reason, :post_turn)}}
      end

    tool_events = tool_events(turn_output)

    tool_results =
      Enum.map(tool_events, fn tool_event ->
        remember_tool_event(target, tool_event, turn_output, memory_opts)
      end)

    tool_successes =
      for {:ok, payload} <- tool_results do
        payload
      end

    tool_failures =
      for {:error, payload} <- tool_results do
        payload
      end

    failures =
      case assistant_result do
        {:error, payload} -> [payload | tool_failures]
        _ -> tool_failures
      end

    cond do
      assistant_failed?(assistant_result) and not allow_partial ->
        {:error, hd(failures).error}

      assistant_failed?(assistant_result) and tool_successes == [] ->
        {:error, hd(failures).error}

      true ->
        {:ok,
         %{
           assistant_memory_id: assistant_memory_id(assistant_result),
           tool_memories: tool_successes,
           tool_memory_ids: Enum.map(tool_successes, & &1.memory_id),
           failures: failures
         }}
    end
  end

  @impl Jido.MemoryOS.FrameworkAdapter
  def normalize_error(reason, phase), do: FrameworkAdapter.normalize_error(reason, phase)

  @spec remember_tool_event(map() | struct(), map(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, map()}
  defp remember_tool_event(target, tool_event, turn_output, memory_opts) do
    event = FrameworkAdapter.normalize_map(tool_event)

    tool_name =
      FrameworkAdapter.map_get(event, :tool_name, FrameworkAdapter.map_get(event, :name, "tool"))

    status = normalize_status(event)

    text =
      FrameworkAdapter.map_get(event, :text) ||
        FrameworkAdapter.map_get(event, :summary) ||
        tool_event_summary(tool_name, status, FrameworkAdapter.map_get(event, :result))

    attrs =
      %{
        class: :episodic,
        kind: :tool_result,
        text: text,
        content: event,
        tags:
          FrameworkAdapter.normalize_tags(FrameworkAdapter.map_get(event, :tags, [])) ++
            [
              "adapter:tool_heavy",
              "tool:" <> sanitize_tag(tool_name),
              "tool_status:" <> sanitize_tag(status)
            ],
        source: "framework.tool:" <> sanitize_tag(tool_name),
        metadata: %{
          "tool_name" => tool_name,
          "tool_status" => status
        }
      }
      |> maybe_put(:chain_id, FrameworkAdapter.map_get(turn_output, :chain_id))
      |> maybe_put(:observed_at, FrameworkAdapter.map_get(event, :observed_at))

    case Jido.MemoryOS.remember(target, attrs, memory_opts) do
      {:ok, record} ->
        {:ok, %{tool_name: tool_name, status: status, record: record, memory_id: record.id}}

      {:error, reason} ->
        {:error,
         %{tool_name: tool_name, status: status, error: normalize_error(reason, :post_turn)}}
    end
  end

  @spec assistant_failed?(tuple()) :: boolean()
  defp assistant_failed?({:error, _payload}), do: true
  defp assistant_failed?(_result), do: false

  @spec assistant_memory_id(tuple()) :: String.t() | nil
  defp assistant_memory_id({:ok, payload}), do: payload.memory_id
  defp assistant_memory_id(_result), do: nil

  @spec tool_names(map() | keyword()) :: [String.t()]
  defp tool_names(turn_input) do
    input = FrameworkAdapter.normalize_map(turn_input)

    tools =
      FrameworkAdapter.map_get(input, :tool_names, FrameworkAdapter.map_get(input, :tools, []))

    tools
    |> normalize_list()
    |> Enum.map(fn
      value when is_binary(value) ->
        value

      value when is_atom(value) ->
        Atom.to_string(value)

      %{} = map ->
        FrameworkAdapter.map_get(map, :name, FrameworkAdapter.map_get(map, :tool_name, "tool"))

      other ->
        to_string(other)
    end)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  @spec tool_events(map() | keyword()) :: [map()]
  defp tool_events(turn_output) do
    turn_output
    |> FrameworkAdapter.normalize_map()
    |> FrameworkAdapter.map_get(:tool_events, [])
    |> normalize_list()
    |> Enum.map(&FrameworkAdapter.normalize_map/1)
    |> Enum.filter(&(map_size(&1) > 0))
  end

  @spec normalize_status(map()) :: String.t()
  defp normalize_status(event) do
    case FrameworkAdapter.map_get(event, :status, FrameworkAdapter.map_get(event, :outcome)) do
      nil ->
        if FrameworkAdapter.map_get(event, :error) in [nil, false], do: "ok", else: "error"

      status when is_atom(status) ->
        Atom.to_string(status)

      status ->
        to_string(status)
    end
  end

  @spec tool_event_summary(String.t(), String.t(), term()) :: String.t()
  defp tool_event_summary(tool_name, status, result) do
    suffix =
      case result do
        nil -> ""
        value when is_binary(value) -> ": " <> String.slice(String.trim(value), 0, 200)
        value -> ": " <> inspect(value, limit: 50)
      end

    "#{tool_name} #{status}#{suffix}"
  end

  @spec maybe_put_empty_tags_any(map()) :: map()
  defp maybe_put_empty_tags_any(query) do
    if FrameworkAdapter.map_get(query, :tags_any, []) == [] do
      Map.delete(query, :tags_any)
    else
      query
    end
  end

  @spec sanitize_tag(String.t()) :: String.t()
  defp sanitize_tag(value) do
    value
    |> to_string()
    |> String.replace(~r/[^a-zA-Z0-9:_\-.]/u, "_")
    |> String.trim("_")
  end

  @spec normalize_list(term()) :: list()
  defp normalize_list(value) when is_list(value), do: value
  defp normalize_list(nil), do: []
  defp normalize_list(value), do: [value]

  @spec maybe_put(map(), atom(), term()) :: map()
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
