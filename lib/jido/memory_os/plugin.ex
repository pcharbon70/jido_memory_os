require Jido.MemoryOS.Actions.Consolidate
require Jido.MemoryOS.Actions.Forget
require Jido.MemoryOS.Actions.Remember
require Jido.MemoryOS.Actions.Retrieve

defmodule Jido.MemoryOS.Plugin do
  @moduledoc """
  MemoryOS plugin entrypoint for Jido agents.

  Phase 5 adds:
  - explicit remember/retrieve/forget/consolidate routes
  - robust plugin state mount/checkpoint/restore behavior
  - signal capture with exact/wildcard matching and rule-based overrides
  """

  alias Jido.MemoryOS.Actions.{Consolidate, Forget, Remember, Retrieve}
  alias Jido.MemoryOS.ErrorMapping
  alias Jido.Signal

  @default_capture_patterns ["ai.react.query", "ai.llm.response", "ai.tool.result"]
  @default_capture_source "jido.signal"
  @default_correlation_prefix "memos"

  @rule_attr_keys [:class, :kind, :source, :text, :content, :metadata, :chain_id, :expires_at]
  @rule_opt_keys [:tier, :namespace, :store, :store_opts, :correlation_id, :server]

  @state_schema Zoi.object(%{
                  config: Zoi.map() |> Zoi.default(%{}),
                  bindings: Zoi.map() |> Zoi.default(%{}),
                  defaults: Zoi.map() |> Zoi.default(%{}),
                  capture: Zoi.map() |> Zoi.default(%{})
                })

  @config_schema Zoi.object(%{
                   config: Zoi.map() |> Zoi.default(%{}),
                   manager: Zoi.any() |> Zoi.optional(),
                   manager_opts: Zoi.any() |> Zoi.default([]),
                   tier: Zoi.any() |> Zoi.default(:short),
                   namespace: Zoi.string() |> Zoi.nullable() |> Zoi.optional(),
                   store: Zoi.any() |> Zoi.optional(),
                   store_opts: Zoi.any() |> Zoi.default([]),
                   correlation_id_prefix:
                     Zoi.string() |> Zoi.default(@default_correlation_prefix),
                   auto_capture: Zoi.boolean() |> Zoi.default(true),
                   capture_signal_patterns:
                     Zoi.list(Zoi.string()) |> Zoi.default(@default_capture_patterns),
                   capture_rules: Zoi.list(Zoi.map()) |> Zoi.default([]),
                   capture_default_tags: Zoi.list(Zoi.string()) |> Zoi.default([]),
                   capture_source: Zoi.string() |> Zoi.default(@default_capture_source),
                   capture_strict: Zoi.boolean() |> Zoi.default(false),
                   capture_tier: Zoi.any() |> Zoi.default(:short),
                   capture_namespace: Zoi.string() |> Zoi.nullable() |> Zoi.optional(),
                   capture_store: Zoi.any() |> Zoi.optional(),
                   capture_store_opts: Zoi.any() |> Zoi.default([]),
                   include_signal_metadata: Zoi.boolean() |> Zoi.default(true)
                 })

  use Jido.Plugin,
    name: "memory_os",
    state_key: :__memory_os__,
    actions: [Remember, Retrieve, Forget, Consolidate],
    signal_routes: [
      {"remember", Remember},
      {"retrieve", Retrieve},
      {"forget", Forget},
      {"consolidate", Consolidate}
    ],
    schema: @state_schema,
    config_schema: @config_schema,
    singleton: true,
    description: "MemoryOS plugin exposing tiered memory actions",
    capabilities: [:memory_os]

  @impl Jido.Plugin
  def mount(_agent, config) do
    {:ok, normalize_state_from_config(config)}
  end

  @impl Jido.Plugin
  def handle_signal(%Signal{} = signal, context) do
    state = plugin_state(context)
    capture = map_get(state, :capture, %{})

    with true <- capture_enabled?(capture),
         true <- matches_capture_patterns?(signal.type, map_get(capture, :patterns, [])),
         {:ok, decision} <- apply_capture_rules(signal, capture),
         false <- decision.skip? do
      attrs = build_capture_attrs(signal, capture, decision)
      opts = build_capture_opts(state, decision)

      case Jido.MemoryOS.remember(map_get(context, :agent, %{}), attrs, opts) do
        {:ok, _record} ->
          {:ok, :continue}

        {:error, reason} ->
          maybe_capture_error(reason, capture)
      end
    else
      false ->
        {:ok, :continue}

      {:error, reason} ->
        maybe_capture_error(reason, capture)

      _other ->
        {:ok, :continue}
    end
  end

  def handle_signal(_signal, _context), do: {:ok, :continue}

  @impl Jido.Plugin
  def on_checkpoint(%{} = plugin_state, _context) do
    if valid_plugin_state?(plugin_state), do: :keep, else: :drop
  end

  def on_checkpoint(_plugin_state, _context), do: :drop

  @impl Jido.Plugin
  def on_restore(nil, _context), do: {:ok, nil}

  def on_restore(%{} = pointer, _context) do
    {:ok, normalize_state(pointer)}
  end

  def on_restore(_pointer, _context), do: {:error, :invalid_plugin_state_pointer}

  @spec normalize_state_from_config(map() | keyword() | term()) :: map()
  defp normalize_state_from_config(config) do
    config_map = normalize_map(config)
    base_tier = map_get(config_map, :tier, :short)

    %{
      config: normalize_map(map_get(config_map, :config, %{})),
      bindings: normalize_manager_state(config_map),
      defaults: normalize_defaults_state(config_map, base_tier),
      capture: normalize_capture_state(config_map, base_tier)
    }
  end

  @spec normalize_state(map() | keyword() | term()) :: map()
  defp normalize_state(state) do
    state_map = normalize_map(state)

    if has_normalized_state_shape?(state_map) do
      base_tier =
        state_map
        |> map_get(:defaults, %{})
        |> map_get(:tier, :short)

      %{
        config: normalize_map(map_get(state_map, :config, %{})),
        bindings:
          normalize_manager_state(
            map_get(state_map, :bindings, map_get(state_map, :manager, %{}))
          ),
        defaults: normalize_defaults_state(map_get(state_map, :defaults, %{}), base_tier),
        capture: normalize_capture_state(map_get(state_map, :capture, %{}), base_tier)
      }
    else
      normalize_state_from_config(state_map)
    end
  end

  @spec has_normalized_state_shape?(map()) :: boolean()
  defp has_normalized_state_shape?(state_map) do
    Map.has_key?(state_map, :defaults) or
      Map.has_key?(state_map, "defaults") or
      Map.has_key?(state_map, :capture) or
      Map.has_key?(state_map, "capture")
  end

  @spec normalize_manager_state(map() | keyword() | term()) :: map()
  defp normalize_manager_state(input) do
    map = normalize_map(input)
    manager_value = map_get(map, :manager)

    {server, manager_opts} =
      cond do
        is_map(manager_value) ->
          {
            map_get(manager_value, :server, map_get(manager_value, :name)),
            normalize_keyword(map_get(manager_value, :opts, map_get(map, :manager_opts, [])))
          }

        is_nil(manager_value) ->
          {map_get(map, :server), normalize_keyword(map_get(map, :manager_opts, []))}

        true ->
          {manager_value, normalize_keyword(map_get(map, :manager_opts, []))}
      end

    %{server: server, opts: manager_opts}
  end

  @spec normalize_defaults_state(map() | keyword() | term(), term()) :: map()
  defp normalize_defaults_state(input, base_tier) do
    map = normalize_map(input)

    %{
      tier: map_get(map, :tier, base_tier),
      namespace: normalize_optional_string(map_get(map, :namespace)),
      store: map_get(map, :store),
      store_opts: normalize_keyword(map_get(map, :store_opts, [])),
      correlation_id_prefix:
        normalize_non_empty_string(
          map_get(map, :correlation_id_prefix),
          @default_correlation_prefix
        )
    }
  end

  @spec normalize_capture_state(map() | keyword() | term(), term()) :: map()
  defp normalize_capture_state(input, base_tier) do
    map = normalize_map(input)

    patterns =
      map_get(map, :capture_signal_patterns, map_get(map, :patterns, @default_capture_patterns))

    %{
      enabled: normalize_boolean(map_get(map, :auto_capture, map_get(map, :enabled, true))),
      patterns: normalize_patterns(patterns),
      rules: normalize_rules(map_get(map, :capture_rules, map_get(map, :rules, []))),
      default_tags:
        normalize_tags(map_get(map, :capture_default_tags, map_get(map, :default_tags, []))),
      source:
        normalize_non_empty_string(
          map_get(map, :capture_source, map_get(map, :source)),
          @default_capture_source
        ),
      strict: normalize_boolean(map_get(map, :capture_strict, map_get(map, :strict, false))),
      include_signal_metadata: normalize_boolean(map_get(map, :include_signal_metadata, true)),
      tier: map_get(map, :capture_tier, map_get(map, :tier, base_tier)),
      namespace:
        normalize_optional_string(map_get(map, :capture_namespace, map_get(map, :namespace))),
      store: map_get(map, :capture_store, map_get(map, :store)),
      store_opts:
        normalize_keyword(map_get(map, :capture_store_opts, map_get(map, :store_opts, [])))
    }
  end

  @spec plugin_state(map()) :: map()
  defp plugin_state(context) do
    state_key =
      context
      |> map_get(:plugin_instance, %{})
      |> map_get(:state_key, __MODULE__.state_key())

    context
    |> map_get(:agent, %{})
    |> map_get(:state, %{})
    |> map_get(state_key, %{})
    |> normalize_state()
  end

  @spec capture_enabled?(map()) :: boolean()
  defp capture_enabled?(capture), do: normalize_boolean(map_get(capture, :enabled, true))

  @spec matches_capture_patterns?(String.t(), [String.t()]) :: boolean()
  defp matches_capture_patterns?(_signal_type, []), do: false

  defp matches_capture_patterns?(signal_type, patterns) when is_binary(signal_type) do
    Enum.any?(patterns, &signal_type_matches?(signal_type, &1))
  end

  defp matches_capture_patterns?(_signal_type, _patterns), do: false

  @spec signal_type_matches?(String.t(), term()) :: boolean()
  defp signal_type_matches?(_signal_type, pattern) when not is_binary(pattern), do: false
  defp signal_type_matches?(_signal_type, "*"), do: true
  defp signal_type_matches?(signal_type, signal_type), do: true

  defp signal_type_matches?(signal_type, pattern) do
    cond do
      String.ends_with?(pattern, ".*") ->
        prefix = String.trim_trailing(pattern, ".*")
        String.starts_with?(signal_type, prefix <> ".")

      String.contains?(pattern, "*") ->
        regex =
          pattern
          |> Regex.escape()
          |> String.replace("\\*", "[^.]*")

        Regex.match?(~r/^#{regex}$/, signal_type)

      true ->
        false
    end
  end

  @spec apply_capture_rules(Signal.t(), map()) ::
          {:ok, %{skip?: boolean(), attrs: map(), opts: map(), tags: [String.t()]}}
  defp apply_capture_rules(%Signal{} = signal, capture) do
    rules = map_get(capture, :rules, [])

    decision =
      Enum.reduce(rules, %{skip?: false, attrs: %{}, opts: %{}, tags: []}, fn rule, acc ->
        if rule_matches_signal?(rule, signal.type) do
          attrs_patch =
            map_get(rule, :attrs, %{})
            |> normalize_map()
            |> Map.merge(pick_rule_fields(rule, @rule_attr_keys))

          opts_patch =
            map_get(rule, :opts, %{})
            |> normalize_map()
            |> Map.merge(pick_rule_fields(rule, @rule_opt_keys))

          tags = normalize_tags(map_get(rule, :tags, []))

          %{
            skip?: acc.skip? or normalize_boolean(map_get(rule, :skip, false)),
            attrs: deep_merge(acc.attrs, attrs_patch),
            opts: Map.merge(acc.opts, opts_patch),
            tags: Enum.uniq(acc.tags ++ tags)
          }
        else
          acc
        end
      end)

    {:ok, decision}
  end

  @spec rule_matches_signal?(map(), String.t()) :: boolean()
  defp rule_matches_signal?(rule, signal_type) do
    case map_get(rule, :pattern) do
      nil -> true
      pattern -> signal_type_matches?(signal_type, pattern)
    end
  end

  @spec build_capture_attrs(Signal.t(), map(), map()) :: map()
  defp build_capture_attrs(%Signal{} = signal, capture, decision) do
    data = normalize_signal_data(signal.data)

    default_text = infer_signal_text(signal.type, data)
    base_text = normalize_non_empty_string(map_get(decision.attrs, :text), default_text)

    base_tags =
      capture
      |> map_get(:default_tags, [])
      |> normalize_tags()
      |> Kernel.++(extract_signal_tags(signal, data))
      |> Kernel.++(decision.tags)
      |> Enum.uniq()

    base_content =
      if normalize_boolean(map_get(capture, :include_signal_metadata, true)) do
        %{
          "signal" => %{
            "id" => signal.id,
            "type" => signal.type,
            "source" => signal.source,
            "subject" => signal.subject,
            "time" => signal.time,
            "extensions" => normalize_map(signal.extensions)
          },
          "data" => data
        }
      else
        data
      end

    metadata =
      map_get(decision.attrs, :metadata, %{})
      |> normalize_map()
      |> Map.put_new("memory_os_capture", %{
        "plugin" => __MODULE__.name(),
        "signal_type" => signal.type,
        "signal_id" => signal.id
      })

    attrs =
      %{
        class: map_get(decision.attrs, :class, :episodic),
        kind: map_get(decision.attrs, :kind, infer_kind(signal.type)),
        text: base_text,
        content: map_get(decision.attrs, :content, base_content),
        tags: base_tags,
        source:
          normalize_non_empty_string(
            map_get(decision.attrs, :source),
            normalize_non_empty_string(
              map_get(capture, :source),
              signal.source || @default_capture_source
            )
          ),
        observed_at: map_get(decision.attrs, :observed_at, signal_time_ms(signal.time)),
        metadata: metadata
      }
      |> maybe_put(:chain_id, map_get(decision.attrs, :chain_id, extract_chain_id(signal, data)))
      |> maybe_put(:expires_at, map_get(decision.attrs, :expires_at))

    final_attrs = Map.merge(attrs, Map.drop(decision.attrs, @rule_attr_keys))

    final_attrs
    |> Map.put(:tags, normalize_tags(map_get(final_attrs, :tags, [])))
    |> Map.put(:text, normalize_non_empty_string(map_get(final_attrs, :text), default_text))
  end

  @spec build_capture_opts(map(), map()) :: keyword()
  defp build_capture_opts(state, decision) do
    defaults = map_get(state, :defaults, %{})
    capture = map_get(state, :capture, %{})
    manager = map_get(state, :bindings, map_get(state, :manager, %{}))
    config = normalize_map(map_get(state, :config, %{}))

    correlation_id =
      map_get(decision.opts, :correlation_id) ||
        "capture-" <>
          normalize_non_empty_string(
            map_get(defaults, :correlation_id_prefix),
            @default_correlation_prefix
          ) <>
          "-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

    base_opts =
      %{
        tier:
          map_get(decision.opts, :tier, map_get(capture, :tier, map_get(defaults, :tier, :short))),
        namespace:
          map_get(
            decision.opts,
            :namespace,
            map_get(capture, :namespace, map_get(defaults, :namespace))
          ),
        store:
          map_get(decision.opts, :store, map_get(capture, :store, map_get(defaults, :store))),
        store_opts:
          map_get(
            decision.opts,
            :store_opts,
            (map_get(defaults, :store_opts, []) |> normalize_keyword()) ++
              (map_get(capture, :store_opts, []) |> normalize_keyword())
          ),
        correlation_id: correlation_id,
        config: config,
        server: map_get(decision.opts, :server, map_get(manager, :server)),
        plugin_state: %{config: config}
      }

    manager_opts = normalize_keyword(map_get(manager, :opts, []))

    manager_opts
    |> Keyword.merge(compact_keyword(base_opts))
  end

  @spec maybe_capture_error(term(), map()) :: {:ok, :continue} | {:error, term()}
  defp maybe_capture_error(reason, capture) do
    if normalize_boolean(map_get(capture, :strict, false)) do
      if jido_error?(reason),
        do: {:error, reason},
        else: {:error, ErrorMapping.from_reason(reason, :remember)}
    else
      {:ok, :continue}
    end
  end

  @spec valid_plugin_state?(map()) :: boolean()
  defp valid_plugin_state?(state) do
    is_map(map_get(state, :bindings, map_get(state, :manager, %{}))) and
      is_map(map_get(state, :defaults, %{})) and
      is_map(map_get(state, :capture, %{}))
  end

  @spec normalize_signal_data(term()) :: map()
  defp normalize_signal_data(%{} = data), do: data

  defp normalize_signal_data(data) when is_list(data) do
    if Keyword.keyword?(data), do: Map.new(data), else: %{"value" => data}
  end

  defp normalize_signal_data(nil), do: %{}
  defp normalize_signal_data(data), do: %{"value" => data}

  @spec extract_signal_tags(Signal.t(), map()) :: [String.t()]
  defp extract_signal_tags(%Signal{} = signal, data) do
    source_tag =
      signal.source
      |> normalize_non_empty_string(nil)
      |> case do
        nil -> []
        value -> ["source:" <> sanitize_tag(value)]
      end

    type_tag = ["signal:" <> sanitize_tag(signal.type)]
    event_tags = normalize_tags(map_get(data, :tags, []))

    tool_tags =
      case normalize_non_empty_string(map_get(data, :tool_name), nil) do
        nil -> []
        tool_name -> ["tool:" <> sanitize_tag(tool_name)]
      end

    source_tag ++ type_tag ++ event_tags ++ tool_tags
  end

  @spec sanitize_tag(String.t()) :: String.t()
  defp sanitize_tag(value) do
    value
    |> String.replace(~r/[^a-zA-Z0-9:_\-.]/u, "_")
    |> String.trim("_")
  end

  @spec infer_signal_text(String.t(), map()) :: String.t()
  defp infer_signal_text(signal_type, data) do
    candidates = [
      map_get(data, :text),
      map_get(data, :query),
      map_get(data, :prompt),
      map_get(data, :response),
      map_get(data, :output),
      map_get(data, :result),
      map_get(data, :message),
      get_in(data, ["text"]),
      get_in(data, ["query"]),
      get_in(data, ["prompt"]),
      get_in(data, ["response"]),
      get_in(data, ["output"]),
      get_in(data, ["result"]),
      get_in(data, ["message"])
    ]

    Enum.find_value(candidates, fn
      value when is_binary(value) and value != "" -> String.trim(value)
      _ -> nil
    end) || "captured signal #{signal_type}"
  end

  @spec infer_kind(String.t()) :: atom()
  defp infer_kind(signal_type) do
    cond do
      String.ends_with?(signal_type, ".query") -> :query
      String.ends_with?(signal_type, ".response") -> :response
      String.contains?(signal_type, ".tool.") -> :tool_result
      String.ends_with?(signal_type, ".result") -> :result
      true -> :event
    end
  end

  @spec extract_chain_id(Signal.t(), map()) :: String.t() | nil
  defp extract_chain_id(%Signal{} = signal, data) do
    map_get(data, :chain_id) ||
      map_get(data, :conversation_id) ||
      map_get(data, :thread_id) ||
      map_get(data, :session_id) ||
      map_get(signal.extensions || %{}, :chain_id) ||
      normalize_optional_string(signal.subject)
  end

  @spec signal_time_ms(term()) :: integer()
  defp signal_time_ms(nil), do: System.system_time(:millisecond)

  defp signal_time_ms(time) when is_integer(time), do: time

  defp signal_time_ms(time) when is_binary(time) do
    case DateTime.from_iso8601(time) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime, :millisecond)
      _ -> System.system_time(:millisecond)
    end
  end

  defp signal_time_ms(_time), do: System.system_time(:millisecond)

  @spec pick_rule_fields(map(), [atom()]) :: map()
  defp pick_rule_fields(rule, keys) do
    Enum.reduce(keys, %{}, fn key, acc ->
      case map_get(rule, key) do
        nil -> acc
        value -> Map.put(acc, key, value)
      end
    end)
  end

  @spec normalize_patterns(term()) :: [String.t()]
  defp normalize_patterns(patterns) when is_list(patterns) do
    patterns
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_patterns(_patterns), do: @default_capture_patterns

  @spec normalize_rules(term()) :: [map()]
  defp normalize_rules(rules) when is_list(rules) do
    rules
    |> Enum.map(&normalize_map/1)
    |> Enum.filter(&(map_size(&1) > 0))
  end

  defp normalize_rules(_rules), do: []

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

  @spec normalize_boolean(term()) :: boolean()
  defp normalize_boolean(value) when is_boolean(value), do: value
  defp normalize_boolean("true"), do: true
  defp normalize_boolean("false"), do: false
  defp normalize_boolean(1), do: true
  defp normalize_boolean(0), do: false
  defp normalize_boolean(_value), do: false

  @spec normalize_non_empty_string(term(), String.t() | nil) :: String.t() | nil
  defp normalize_non_empty_string(value, fallback)

  defp normalize_non_empty_string(value, fallback) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: fallback, else: trimmed
  end

  defp normalize_non_empty_string(nil, fallback), do: fallback

  defp normalize_non_empty_string(value, fallback),
    do: normalize_non_empty_string(to_string(value), fallback)

  @spec normalize_optional_string(term()) :: String.t() | nil
  defp normalize_optional_string(value), do: normalize_non_empty_string(value, nil)

  @spec normalize_keyword(term()) :: keyword()
  defp normalize_keyword(opts) when is_list(opts), do: opts
  defp normalize_keyword(%{} = opts), do: Enum.to_list(opts)
  defp normalize_keyword(_opts), do: []

  @spec compact_keyword(map()) :: keyword()
  defp compact_keyword(map) do
    map
    |> Enum.reduce([], fn
      {_key, nil}, acc ->
        acc

      {:store_opts, value}, acc ->
        [{:store_opts, normalize_keyword(value)} | acc]

      {key, value}, acc ->
        [{key, value} | acc]
    end)
    |> Enum.reverse()
  end

  @spec deep_merge(map(), map()) :: map()
  defp deep_merge(left, right) do
    Map.merge(left, right, fn _key, left_value, right_value ->
      if is_map(left_value) and is_map(right_value) do
        deep_merge(left_value, right_value)
      else
        right_value
      end
    end)
  end

  @spec maybe_put(map(), atom(), term()) :: map()
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

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

  @spec jido_error?(term()) :: boolean()
  defp jido_error?(%{__struct__: module}) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.Jido.Error.")
  end

  defp jido_error?(_reason), do: false
end
