defmodule Jido.MemoryOS.FrameworkAdapter do
  @moduledoc """
  Adapter contract for framework-style agent loops.

  Adapters bridge external request/response orchestration with MemoryOS by
  exposing:

  - `pre_turn/3` retrieval hooks
  - `post_turn/3` memory ingestion hooks
  - `normalize_error/2` consistent error mapping
  """

  alias Jido.MemoryOS.{Compatibility, ErrorMapping}

  @memory_opt_keys [
    :tier,
    :namespace,
    :correlation_id,
    :store,
    :store_opts,
    :mem_os,
    :server,
    :config,
    :app_config,
    :agent_id,
    :tier_mode,
    :context_token_budget,
    :semantic_provider,
    :semantic_timeout_ms,
    :timeout_ms,
    :call_timeout,
    :limit
  ]

  @type turn_payload :: map() | keyword()
  @type hook_result :: {:ok, map()} | {:error, term()}

  @callback pre_turn(target :: map() | struct(), turn_input :: turn_payload(), opts :: keyword()) ::
              hook_result()
  @callback post_turn(
              target :: map() | struct(),
              turn_output :: turn_payload(),
              opts :: keyword()
            ) ::
              hook_result()
  @callback normalize_error(reason :: term(), phase :: atom()) :: term()

  @doc """
  Returns known MemoryOS option keys.
  """
  @spec memory_opt_keys() :: [atom()]
  def memory_opt_keys, do: @memory_opt_keys

  @doc """
  Normalizes framework adapter options and payload-level memory options.
  """
  @spec memory_opts(keyword() | map(), map() | keyword()) :: keyword()
  def memory_opts(adapter_opts, payload \\ %{}) do
    adapter_opts =
      adapter_opts
      |> normalize_keyword()
      |> pick_memory_opts()

    payload_map = normalize_map(payload)

    payload_memory_opts =
      payload_map
      |> map_get(:memory_opts, [])
      |> normalize_keyword()
      |> pick_memory_opts()

    payload_inline_opts =
      Enum.reduce(@memory_opt_keys, [], fn key, acc ->
        case map_get(payload_map, key) do
          nil -> acc
          value -> [{key, value} | acc]
        end
      end)
      |> Enum.reverse()

    adapter_opts
    |> Keyword.merge(payload_memory_opts)
    |> Keyword.merge(payload_inline_opts)
  end

  @doc """
  Builds a retrieval query from common framework payload shapes.
  """
  @spec build_query(map() | keyword(), pos_integer()) :: map()
  def build_query(payload, default_limit \\ 6) do
    payload_map = normalize_map(payload)

    query =
      case map_get(payload_map, :memory_query) do
        nil ->
          %{
            text_contains:
              map_get(payload_map, :query) ||
                map_get(payload_map, :prompt) ||
                map_get(payload_map, :text),
            tags_any: map_get(payload_map, :tags),
            limit:
              map_get(payload_map, :memory_limit, map_get(payload_map, :limit, default_limit))
          }

        query_payload ->
          Compatibility.map_legacy_query(query_payload)
      end

    query
    |> compact_map()
    |> normalize_query_defaults(default_limit)
  end

  @doc """
  Builds standard remember attrs from turn outputs.
  """
  @spec build_memory_attrs(map() | keyword(), map() | keyword()) :: map()
  def build_memory_attrs(payload, defaults \\ %{}) do
    payload_map = normalize_map(payload)
    defaults_map = normalize_map(defaults)

    text =
      text_from_payload(payload_map) ||
        normalize_non_empty_string(map_get(defaults_map, :text), "framework turn event")

    tags =
      defaults_map
      |> map_get(:default_tags, [])
      |> normalize_tags()
      |> Kernel.++(normalize_tags(map_get(payload_map, :tags, [])))
      |> Enum.uniq()

    attrs =
      %{
        class: map_get(defaults_map, :class, :episodic),
        kind: map_get(defaults_map, :kind, :event),
        text: text,
        content: map_get(payload_map, :content, payload_map),
        tags: tags,
        source:
          normalize_non_empty_string(
            map_get(payload_map, :source),
            normalize_non_empty_string(map_get(defaults_map, :source), "framework.adapter")
          ),
        metadata:
          map_get(defaults_map, :metadata, %{})
          |> normalize_map()
          |> Map.put_new("framework_adapter", "memory_os"),
        observed_at: map_get(payload_map, :observed_at)
      }
      |> maybe_put(:chain_id, map_get(payload_map, :chain_id))
      |> maybe_put(:expires_at, map_get(payload_map, :expires_at))

    compact_map(attrs)
  end

  @doc """
  Normalizes framework errors into MemoryOS/Jido-native error structs.
  """
  @spec normalize_error(term(), atom()) :: term()
  def normalize_error(reason, phase) do
    cond do
      jido_error?(reason) ->
        reason

      phase in [:pre_turn, :retrieve, :explain_retrieval] ->
        ErrorMapping.from_reason(reason, :retrieve)

      phase in [:post_turn, :remember] ->
        ErrorMapping.from_reason(reason, :remember)

      true ->
        ErrorMapping.from_reason(reason, phase)
    end
  end

  @doc """
  Normalizes map-like payloads.
  """
  @spec normalize_map(term()) :: map()
  def normalize_map(%{} = map), do: map

  def normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  def normalize_map(_value), do: %{}

  @doc """
  Reads atom/string keys from a map.
  """
  @spec map_get(map(), atom() | String.t(), term()) :: term()
  def map_get(map, key, default \\ nil)

  def map_get(map, key, default) when is_atom(key),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  def map_get(map, key, default) when is_binary(key) do
    case Enum.find(map, fn
           {atom_key, _value} when is_atom(atom_key) -> Atom.to_string(atom_key) == key
           _ -> false
         end) do
      {_, value} -> value
      nil -> Map.get(map, key, default)
    end
  end

  @doc """
  Normalizes any tag-like payload to list-of-strings.
  """
  @spec normalize_tags(term()) :: [String.t()]
  def normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(fn
      tag when is_binary(tag) -> String.trim(tag)
      tag when is_atom(tag) -> tag |> Atom.to_string() |> String.trim()
      tag -> tag |> to_string() |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  def normalize_tags(tag) when is_binary(tag), do: normalize_tags([tag])
  def normalize_tags(tag) when is_atom(tag), do: normalize_tags([tag])
  def normalize_tags(_tags), do: []

  @doc """
  Normalizes keywords from list or map input.
  """
  @spec normalize_keyword(term()) :: keyword()
  def normalize_keyword(opts) when is_list(opts), do: opts
  def normalize_keyword(%{} = opts), do: Enum.to_list(opts)
  def normalize_keyword(_opts), do: []

  @doc """
  Best-effort text extraction from turn payloads.
  """
  @spec text_from_payload(map() | keyword()) :: String.t() | nil
  def text_from_payload(payload) do
    payload_map = normalize_map(payload)

    candidates = [
      map_get(payload_map, :text),
      map_get(payload_map, :response_text),
      map_get(payload_map, :response),
      map_get(payload_map, :output),
      map_get(payload_map, :query),
      map_get(payload_map, :prompt),
      map_get(payload_map, :message),
      get_in(payload_map, ["text"]),
      get_in(payload_map, ["response_text"]),
      get_in(payload_map, ["response"]),
      get_in(payload_map, ["output"]),
      get_in(payload_map, ["query"]),
      get_in(payload_map, ["prompt"]),
      get_in(payload_map, ["message"])
    ]

    Enum.find_value(candidates, fn
      value when is_binary(value) ->
        trimmed = String.trim(value)
        if trimmed == "", do: nil, else: trimmed

      _ ->
        nil
    end)
  end

  @doc """
  Returns target id from map/struct payload.
  """
  @spec target_id(map() | struct()) :: String.t() | nil
  def target_id(target) do
    map_get(normalize_map(target), :id)
  end

  @spec pick_memory_opts(keyword()) :: keyword()
  defp pick_memory_opts(opts) do
    Enum.filter(opts, fn {key, _value} -> key in @memory_opt_keys end)
  end

  @spec normalize_query_defaults(map(), pos_integer()) :: map()
  defp normalize_query_defaults(query, default_limit) do
    limit =
      case map_get(query, :limit) do
        integer when is_integer(integer) and integer > 0 -> integer
        binary when is_binary(binary) -> parse_positive_integer(binary, default_limit)
        _ -> default_limit
      end

    query
    |> Map.put(:limit, limit)
    |> update_query_tags()
  end

  @spec update_query_tags(map()) :: map()
  defp update_query_tags(query) do
    case map_get(query, :tags_any) do
      nil -> query
      tags -> Map.put(query, :tags_any, normalize_tags(tags))
    end
  end

  @spec compact_map(map()) :: map()
  defp compact_map(map) do
    Enum.reduce(map, %{}, fn
      {_key, nil}, acc -> acc
      {key, value}, acc -> Map.put(acc, key, value)
    end)
  end

  @spec parse_positive_integer(String.t(), pos_integer()) :: pos_integer()
  defp parse_positive_integer(value, fallback) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> integer
      _ -> fallback
    end
  end

  @spec maybe_put(map(), atom(), term()) :: map()
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @spec normalize_non_empty_string(term(), String.t() | nil) :: String.t() | nil
  defp normalize_non_empty_string(value, fallback)

  defp normalize_non_empty_string(value, fallback) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: fallback, else: trimmed
  end

  defp normalize_non_empty_string(nil, fallback), do: fallback

  defp normalize_non_empty_string(value, fallback),
    do: normalize_non_empty_string(to_string(value), fallback)

  @spec jido_error?(term()) :: boolean()
  defp jido_error?(%{__struct__: module}) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.Jido.Error.")
  end

  defp jido_error?(_reason), do: false
end
