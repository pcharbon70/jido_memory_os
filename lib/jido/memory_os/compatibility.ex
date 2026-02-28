defmodule Jido.MemoryOS.Compatibility do
  @moduledoc """
  Dependency compatibility and migration helpers for `jido_memory`.

  MemoryOS keeps `jido_memory 0.1.x` pinned and validates runtime capabilities.
  It also provides legacy payload mappers to smooth migration from direct
  `jido_memory` usage.
  """

  @runtime_module Jido.Memory.Runtime
  @version_requirement "~> 0.1.0"
  @required_runtime_functions [
    remember: 3,
    get: 3,
    recall: 2,
    forget: 3,
    prune_expired: 2,
    resolve_runtime: 3
  ]

  @legacy_option_keys [
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
    :timeout_ms,
    :call_timeout
  ]

  @legacy_query_key_map %{
    q: :text_contains,
    query: :text_contains,
    text: :text_contains,
    tag: :tags_any,
    tags: :tags_any,
    max_results: :limit,
    start_time: :since,
    end_time: :until,
    sort: :order
  }

  @result_key_aliases %{
    memory_results: :memories,
    last_memory_id: :memory_id,
    last_memory_deleted?: :deleted?,
    memory_consolidation: :consolidation
  }

  case Code.ensure_compiled(@runtime_module) do
    {:module, _module} ->
      :ok

    {:error, _reason} ->
      raise CompileError,
        description: "missing required dependency module #{inspect(@runtime_module)}"
  end

  for {function_name, arity} <- @required_runtime_functions do
    if not function_exported?(@runtime_module, function_name, arity) do
      raise CompileError,
        description:
          "missing #{inspect(@runtime_module)}.#{function_name}/#{arity} required by Jido.MemoryOS"
    end
  end

  @doc """
  Returns the required `jido_memory` version constraint.
  """
  @spec version_requirement() :: String.t()
  def version_requirement, do: @version_requirement

  @doc """
  Returns required runtime function capabilities.
  """
  @spec required_runtime_functions() :: keyword(pos_integer())
  def required_runtime_functions, do: @required_runtime_functions

  @doc """
  Validates both `jido_memory` version and required runtime functions.
  """
  @spec validate_runtime_contract() :: :ok | {:error, term()}
  def validate_runtime_contract do
    with {:ok, _version} <- validate_jido_memory_version(),
         :ok <- validate_required_capabilities(@runtime_module) do
      :ok
    end
  end

  @doc """
  Validates only required functions on a runtime module.
  """
  @spec validate_required_capabilities(module()) :: :ok | {:error, term()}
  def validate_required_capabilities(module) when is_atom(module) do
    missing =
      @required_runtime_functions
      |> Enum.reject(fn {function_name, arity} ->
        function_exported?(module, function_name, arity)
      end)

    if missing == [] do
      :ok
    else
      {:error, {:missing_runtime_capabilities, module, missing}}
    end
  end

  @doc """
  Validates loaded `jido_memory` application version against supported range.
  """
  @spec validate_jido_memory_version() :: {:ok, String.t()} | {:error, term()}
  def validate_jido_memory_version do
    case Application.spec(:jido_memory, :vsn) do
      nil ->
        {:error, :jido_memory_not_loaded}

      version when is_list(version) ->
        validate_version_string(to_string(version))

      version when is_binary(version) ->
        validate_version_string(version)

      version ->
        {:error, {:invalid_jido_memory_version, version}}
    end
  end

  @doc """
  Maps a legacy write payload into MemoryOS remember attrs and options.

  The returned shape is `%{attrs: map(), opts: keyword()}`.
  """
  @spec map_legacy_input(map() | keyword()) :: %{attrs: map(), opts: keyword()}
  def map_legacy_input(payload) do
    map = normalize_map(payload)
    opts = extract_legacy_opts(map)

    attrs =
      map
      |> drop_option_keys(@legacy_option_keys)
      |> normalize_legacy_attrs()

    %{attrs: attrs, opts: opts}
  end

  @doc """
  Maps a legacy recall query into a MemoryOS retrieve query.
  """
  @spec map_legacy_query(map() | keyword()) :: map()
  def map_legacy_query(query) do
    map = normalize_map(query)

    base =
      Enum.reduce(map, %{}, fn {raw_key, value}, acc ->
        case normalize_query_key(raw_key) do
          nil ->
            acc

          key ->
            Map.put(acc, key, normalize_query_value(key, value))
        end
      end)

    case map_get(map, :filters) do
      nil ->
        base

      filters when is_map(filters) or is_list(filters) ->
        Map.merge(base, map_legacy_query(filters))

      _other ->
        base
    end
  end

  @doc """
  Maps MemoryOS action/result shapes to legacy-friendly key names.

  Supported mode hints:
  - `:remember`
  - `:retrieve`
  - `:forget`
  - `:consolidate`
  - `:auto` (default)
  """
  @spec map_legacy_result(term(), keyword()) :: map()
  def map_legacy_result(result, opts \\ []) do
    mode = Keyword.get(opts, :mode, :auto)
    mapped = to_legacy_result_map(result)
    mapped = alias_result_keys(mapped)
    apply_mode_hints(mapped, mode)
  end

  @spec validate_version_string(String.t()) :: {:ok, String.t()} | {:error, term()}
  defp validate_version_string(version) do
    if Version.match?(version, @version_requirement) do
      {:ok, version}
    else
      {:error, {:unsupported_jido_memory_version, version, @version_requirement}}
    end
  end

  @spec extract_legacy_opts(map()) :: keyword()
  defp extract_legacy_opts(map) do
    Enum.reduce(@legacy_option_keys, [], fn key, acc ->
      case map_get(map, key) do
        nil -> acc
        value -> [{key, value} | acc]
      end
    end)
    |> Enum.reverse()
  end

  @spec drop_option_keys(map(), [atom()]) :: map()
  defp drop_option_keys(map, keys) do
    Enum.reduce(keys, map, fn key, acc ->
      acc
      |> Map.delete(key)
      |> Map.delete(Atom.to_string(key))
    end)
  end

  @spec normalize_legacy_attrs(map()) :: map()
  defp normalize_legacy_attrs(attrs) do
    attrs
    |> maybe_promote_legacy_content()
    |> maybe_promote_legacy_text()
    |> maybe_normalize_tags()
    |> maybe_normalize_metadata()
    |> maybe_normalize_timestamp(:observed_at)
    |> maybe_normalize_timestamp(:expires_at)
  end

  @spec maybe_promote_legacy_content(map()) :: map()
  defp maybe_promote_legacy_content(attrs) do
    content = map_get(attrs, :content) || map_get(attrs, :payload) || map_get(attrs, :data)
    if is_nil(content), do: attrs, else: Map.put(attrs, :content, content)
  end

  @spec maybe_promote_legacy_text(map()) :: map()
  defp maybe_promote_legacy_text(attrs) do
    case map_get(attrs, :text) do
      value when is_binary(value) and value != "" ->
        attrs

      _ ->
        promoted =
          map_get(attrs, :query) ||
            map_get(attrs, :prompt) ||
            map_get(attrs, :message) ||
            map_get(attrs, :content)

        if is_binary(promoted) and promoted != "" do
          Map.put(attrs, :text, promoted)
        else
          attrs
        end
    end
  end

  @spec maybe_normalize_tags(map()) :: map()
  defp maybe_normalize_tags(attrs) do
    case map_get(attrs, :tags) do
      nil ->
        attrs

      tags ->
        Map.put(attrs, :tags, normalize_tags(tags))
    end
  end

  @spec maybe_normalize_metadata(map()) :: map()
  defp maybe_normalize_metadata(attrs) do
    case map_get(attrs, :metadata) do
      nil -> attrs
      metadata -> Map.put(attrs, :metadata, normalize_map(metadata))
    end
  end

  @spec maybe_normalize_timestamp(map(), atom()) :: map()
  defp maybe_normalize_timestamp(attrs, field) do
    case map_get(attrs, field) do
      value when is_integer(value) ->
        attrs

      value when is_binary(value) ->
        case Integer.parse(value) do
          {integer, ""} -> Map.put(attrs, field, integer)
          _ -> attrs
        end

      _ ->
        attrs
    end
  end

  @spec normalize_query_key(term()) :: atom() | String.t() | nil
  defp normalize_query_key(key) when is_atom(key) do
    Map.get(@legacy_query_key_map, key, key)
  end

  defp normalize_query_key(key) when is_binary(key) do
    known_key =
      Enum.find(@legacy_query_key_map, fn {legacy, _mapped} ->
        Atom.to_string(legacy) == key
      end)

    case known_key do
      {_legacy, mapped} -> mapped
      nil -> key
    end
  end

  defp normalize_query_key(_key), do: nil

  @spec normalize_query_value(atom() | String.t(), term()) :: term()
  defp normalize_query_value(key, value)
       when key in [:tags_any, :tags_all, "tags_any", "tags_all"] do
    normalize_tags(value)
  end

  defp normalize_query_value(key, value) when key in [:classes, :kinds, "classes", "kinds"] do
    normalize_list(value)
  end

  defp normalize_query_value(key, value) when key in [:limit, "limit"] do
    case value do
      integer when is_integer(integer) and integer > 0 ->
        integer

      binary when is_binary(binary) ->
        case Integer.parse(binary) do
          {integer, ""} when integer > 0 -> integer
          _ -> value
        end

      _ ->
        value
    end
  end

  defp normalize_query_value(key, value) when key in [:order, "order"] do
    case value do
      :asc -> :asc
      :desc -> :desc
      "asc" -> :asc
      "desc" -> :desc
      _ -> value
    end
  end

  defp normalize_query_value(key, value) when key in [:since, :until, "since", "until"] do
    case value do
      integer when is_integer(integer) ->
        integer

      binary when is_binary(binary) ->
        case Integer.parse(binary) do
          {integer, ""} -> integer
          _ -> value
        end

      _ ->
        value
    end
  end

  defp normalize_query_value(_key, value), do: value

  @spec to_legacy_result_map(term()) :: map()
  defp to_legacy_result_map(%{} = result), do: result
  defp to_legacy_result_map(records) when is_list(records), do: %{memories: records}

  defp to_legacy_result_map(%Jido.Memory.Record{} = record),
    do: %{memory: record, memory_id: record.id}

  defp to_legacy_result_map(value) when is_boolean(value), do: %{deleted?: value}
  defp to_legacy_result_map(value), do: %{result: value}

  @spec alias_result_keys(map()) :: map()
  defp alias_result_keys(map) do
    Enum.reduce(@result_key_aliases, map, fn {from_key, to_key}, acc ->
      case map_get(acc, from_key) do
        nil ->
          acc

        value ->
          acc
          |> put_if_missing(to_key, value)
          |> put_if_missing(Atom.to_string(to_key), value)
      end
    end)
  end

  @spec apply_mode_hints(map(), atom()) :: map()
  defp apply_mode_hints(result, :remember) do
    case map_get(result, :memory) do
      %Jido.Memory.Record{} = record -> put_if_missing(result, :memory_id, record.id)
      _ -> result
    end
  end

  defp apply_mode_hints(result, :retrieve),
    do: put_if_missing(result, :memories, map_get(result, :memory_results, []))

  defp apply_mode_hints(result, :forget),
    do: put_if_missing(result, :deleted?, map_get(result, :last_memory_deleted?))

  defp apply_mode_hints(result, :consolidate),
    do: put_if_missing(result, :consolidation, map_get(result, :memory_consolidation))

  defp apply_mode_hints(result, _mode), do: result

  @spec put_if_missing(map(), atom() | String.t(), term()) :: map()
  defp put_if_missing(map, _key, nil), do: map

  defp put_if_missing(map, key, value) do
    if map_get(map, key) == nil do
      Map.put(map, key, value)
    else
      map
    end
  end

  @spec normalize_tags(term()) :: [String.t()]
  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(fn
      tag when is_binary(tag) -> String.trim(tag)
      tag when is_atom(tag) -> tag |> Atom.to_string() |> String.trim()
      tag -> tag |> to_string() |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_tags(tag) when is_binary(tag), do: normalize_tags([tag])
  defp normalize_tags(tag) when is_atom(tag), do: normalize_tags([tag])
  defp normalize_tags(_tags), do: []

  @spec normalize_list(term()) :: list()
  defp normalize_list(value) when is_list(value), do: value
  defp normalize_list(nil), do: []
  defp normalize_list(value), do: [value]

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_value), do: %{}

  @spec map_get(map(), atom() | String.t(), term()) :: term()
  defp map_get(map, key, default \\ nil)

  defp map_get(map, key, default) when is_atom(key),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp map_get(map, key, default) when is_binary(key) do
    case Enum.find(map, fn
           {atom_key, _value} when is_atom(atom_key) -> Atom.to_string(atom_key) == key
           _ -> false
         end) do
      {_, value} -> value
      nil -> Map.get(map, key, default)
    end
  end
end
