defmodule Jido.MemoryOS.Config do
  @moduledoc """
  Runtime configuration contract for MemoryOS.

  Precedence is deterministic:
  1. per-call overrides
  2. plugin state overrides
  3. application config (`Application.get_env/3`)
  """

  alias Jido.MemoryOS.ConfigError

  @tiers [:short, :mid, :long]
  @root_keys [:namespace_template, :tiers, :retrieval, :plugin, :safety]
  @tier_keys [:namespace_suffix, :store, :store_opts, :max_records, :ttl_ms, :promotion_threshold]
  @retrieval_keys [:limit, :ranking, :fallback]
  @ranking_keys [:lexical_weight, :semantic_weight]
  @plugin_keys [:route_prefix, :auto_capture, :capture_signal_patterns]
  @safety_keys [:audit_enabled, :redaction_enabled, :pii_strategy]

  @defaults %{
    namespace_template: "agent:%{agent_id}:%{tier}",
    tiers: %{
      short: %{
        namespace_suffix: "short",
        store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_short]},
        max_records: 200,
        ttl_ms: 3_600_000,
        promotion_threshold: 0.2
      },
      mid: %{
        namespace_suffix: "mid",
        store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_mid]},
        max_records: 2_000,
        ttl_ms: 86_400_000,
        promotion_threshold: 0.5
      },
      long: %{
        namespace_suffix: "long",
        store: {Jido.Memory.Store.ETS, [table: :jido_memory_os_long]},
        max_records: 20_000,
        ttl_ms: 2_592_000_000,
        promotion_threshold: 0.8
      }
    },
    retrieval: %{
      limit: 20,
      ranking: %{lexical_weight: 0.7, semantic_weight: 0.3},
      fallback: :lexical
    },
    plugin: %{
      route_prefix: "memory_os",
      auto_capture: true,
      capture_signal_patterns: ["ai.react.query", "ai.llm.response", "ai.tool.result"]
    },
    safety: %{
      audit_enabled: true,
      redaction_enabled: true,
      pii_strategy: :mask
    }
  }

  @type tier :: :short | :mid | :long
  @type t :: %{
          namespace_template: String.t(),
          tiers: %{
            short: map(),
            mid: map(),
            long: map()
          },
          retrieval: map(),
          plugin: map(),
          safety: map()
        }

  @doc """
  Returns default validated configuration.
  """
  @spec defaults() :: t()
  def defaults, do: @defaults

  @doc """
  Reads application-level overrides for MemoryOS config.
  """
  @spec app_config() :: map()
  def app_config, do: Application.get_env(:jido_memory_os, __MODULE__, %{})

  @doc """
  Resolves effective config using call/plugin/app precedence.
  """
  @spec resolve(keyword() | map(), map(), map() | keyword()) ::
          {:ok, t()} | {:error, [ConfigError.t()]}
  def resolve(call_overrides \\ [], plugin_state \\ %{}, app_overrides \\ app_config()) do
    merged =
      app_overrides
      |> normalize_input()
      |> deep_merge(extract_config_overrides(plugin_state))
      |> deep_merge(extract_config_overrides(call_overrides))

    validate(merged)
  end

  @doc """
  Validates and normalizes config values, including tier stores.
  """
  @spec validate(map() | keyword()) :: {:ok, t()} | {:error, [ConfigError.t()]}
  def validate(input) do
    config = normalize_input(input)
    {root_overrides, root_unknown} = normalize_known_keys(config, @root_keys)

    errors =
      root_unknown
      |> Enum.map(&error([to_path_key(&1)], :unknown_field, "unknown configuration field", &1))

    {namespace_template, errors} =
      validate_namespace_template(Map.get(root_overrides, :namespace_template), errors)

    {tiers, errors} = validate_tiers(Map.get(root_overrides, :tiers), errors)
    {retrieval, errors} = validate_retrieval(Map.get(root_overrides, :retrieval), errors)
    {plugin, errors} = validate_plugin(Map.get(root_overrides, :plugin), errors)
    {safety, errors} = validate_safety(Map.get(root_overrides, :safety), errors)

    if errors == [] do
      {:ok,
       %{
         namespace_template: namespace_template,
         tiers: tiers,
         retrieval: retrieval,
         plugin: plugin,
         safety: safety
       }}
    else
      {:error, Enum.reverse(errors)}
    end
  end

  @doc """
  Returns tier config for one tier.
  """
  @spec tier_config(t(), tier()) :: {:ok, map()} | {:error, term()}
  def tier_config(%{tiers: tiers}, tier) when tier in @tiers do
    case Map.fetch(tiers, tier) do
      {:ok, tier_config} -> {:ok, tier_config}
      :error -> {:error, {:unknown_tier, tier}}
    end
  end

  def tier_config(_config, tier), do: {:error, {:unknown_tier, tier}}

  @doc """
  Resolves namespace for a tier using template and agent id.
  """
  @spec tier_namespace(t(), String.t() | nil, tier(), String.t() | nil) ::
          {:ok, String.t()} | {:error, term()}
  def tier_namespace(config, agent_id, tier, explicit_namespace \\ nil)

  def tier_namespace(_config, _agent_id, _tier, explicit_namespace)
      when is_binary(explicit_namespace) do
    namespace = String.trim(explicit_namespace)
    if namespace == "", do: {:error, :namespace_required}, else: {:ok, namespace}
  end

  def tier_namespace(%{namespace_template: template}, agent_id, tier, _explicit)
      when tier in @tiers and is_binary(agent_id) and agent_id != "" do
    namespace =
      template
      |> String.replace("%{agent_id}", agent_id)
      |> String.replace("%{tier}", Atom.to_string(tier))

    {:ok, namespace}
  end

  def tier_namespace(_config, _agent_id, _tier, _explicit), do: {:error, :namespace_required}

  @doc """
  Resolves normalized tier store declaration.
  """
  @spec tier_store(t(), tier()) :: {:ok, {module(), keyword()}} | {:error, term()}
  def tier_store(config, tier) do
    with {:ok, tier_config} <- tier_config(config, tier),
         {:ok, store} <- Map.fetch(tier_config, :store) do
      {:ok, store}
    else
      :error -> {:error, {:missing_tier_store, tier}}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Extracts only config keys from call options or plugin state.
  """
  @spec extract_config_overrides(keyword() | map()) :: map()
  def extract_config_overrides(source) do
    normalized = normalize_input(source)
    nested_config = normalize_input(map_get(normalized, :config, %{}))
    {root, _unknown} = normalize_known_keys(normalized, @root_keys)
    deep_merge(nested_config, root)
  end

  @spec validate_namespace_template(term(), [ConfigError.t()]) :: {String.t(), [ConfigError.t()]}
  defp validate_namespace_template(nil, errors), do: {@defaults.namespace_template, errors}

  defp validate_namespace_template(value, errors) when is_binary(value) do
    trimmed = String.trim(value)

    cond do
      trimmed == "" ->
        {@defaults.namespace_template,
         [
           error([:namespace_template], :invalid_value, "must be a non-empty string", value)
           | errors
         ]}

      not String.contains?(trimmed, "%{agent_id}") ->
        {@defaults.namespace_template,
         [
           error(
             [:namespace_template],
             :invalid_template,
             "must include %{agent_id} placeholder",
             value
           )
           | errors
         ]}

      not String.contains?(trimmed, "%{tier}") ->
        {@defaults.namespace_template,
         [
           error(
             [:namespace_template],
             :invalid_template,
             "must include %{tier} placeholder",
             value
           )
           | errors
         ]}

      true ->
        {trimmed, errors}
    end
  end

  defp validate_namespace_template(value, errors) do
    {@defaults.namespace_template,
     [error([:namespace_template], :invalid_type, "must be a string", value) | errors]}
  end

  @spec validate_tiers(term(), [ConfigError.t()]) :: {map(), [ConfigError.t()]}
  defp validate_tiers(value, errors) do
    {input, errors} = normalize_section_map(value, [:tiers], errors)
    {normalized, unknown} = normalize_known_keys(input, @tiers)

    errors =
      Enum.reduce(unknown, errors, fn key, acc ->
        [error([:tiers, to_path_key(key)], :unknown_field, "unknown tier key", key) | acc]
      end)

    Enum.reduce(@tiers, {%{}, errors}, fn tier, {acc, acc_errors} ->
      tier_default = Map.fetch!(@defaults.tiers, tier)
      tier_input = map_get(normalized, tier, %{})
      {tier_map, tier_errors} = validate_one_tier(tier, tier_input, tier_default, acc_errors)
      {Map.put(acc, tier, tier_map), tier_errors}
    end)
  end

  @spec validate_one_tier(tier(), term(), map(), [ConfigError.t()]) ::
          {map(), [ConfigError.t()]}
  defp validate_one_tier(tier, value, default, errors) do
    path = [:tiers, tier]
    {input, errors} = normalize_section_map(value, path, errors)
    {normalized, unknown} = normalize_known_keys(input, @tier_keys)

    errors =
      Enum.reduce(unknown, errors, fn key, acc ->
        [error(path ++ [to_path_key(key)], :unknown_field, "unknown tier config key", key) | acc]
      end)

    namespace_suffix =
      map_get(normalized, :namespace_suffix, default.namespace_suffix)

    {namespace_suffix, errors} =
      validate_non_empty_string(
        namespace_suffix,
        path ++ [:namespace_suffix],
        errors,
        default.namespace_suffix
      )

    max_records = map_get(normalized, :max_records, default.max_records)

    {max_records, errors} =
      validate_positive_integer(max_records, path ++ [:max_records], errors, default.max_records)

    ttl_ms = map_get(normalized, :ttl_ms, default.ttl_ms)

    {ttl_ms, errors} =
      validate_positive_integer(ttl_ms, path ++ [:ttl_ms], errors, default.ttl_ms)

    promotion_threshold = map_get(normalized, :promotion_threshold, default.promotion_threshold)

    {promotion_threshold, errors} =
      validate_unit_number(
        promotion_threshold,
        path ++ [:promotion_threshold],
        errors,
        default.promotion_threshold
      )

    store_value = map_get(normalized, :store, default.store)
    store_opts = map_get(normalized, :store_opts, [])
    {store, errors} = validate_store(store_value, store_opts, path, errors, default.store)

    {%{
       namespace_suffix: namespace_suffix,
       store: store,
       max_records: max_records,
       ttl_ms: ttl_ms,
       promotion_threshold: promotion_threshold
     }, errors}
  end

  @spec validate_retrieval(term(), [ConfigError.t()]) :: {map(), [ConfigError.t()]}
  defp validate_retrieval(value, errors) do
    path = [:retrieval]
    {input, errors} = normalize_section_map(value, path, errors)
    {normalized, unknown} = normalize_known_keys(input, @retrieval_keys)

    errors =
      Enum.reduce(unknown, errors, fn key, acc ->
        [
          error(path ++ [to_path_key(key)], :unknown_field, "unknown retrieval config key", key)
          | acc
        ]
      end)

    limit = map_get(normalized, :limit, @defaults.retrieval.limit)

    {limit, errors} =
      validate_positive_integer(limit, path ++ [:limit], errors, @defaults.retrieval.limit)

    ranking_default = @defaults.retrieval.ranking

    {ranking_input, errors} =
      normalize_section_map(map_get(normalized, :ranking), path ++ [:ranking], errors)

    {ranking_input, ranking_unknown} = normalize_known_keys(ranking_input, @ranking_keys)

    errors =
      Enum.reduce(ranking_unknown, errors, fn key, acc ->
        [
          error(path ++ [:ranking, to_path_key(key)], :unknown_field, "unknown ranking key", key)
          | acc
        ]
      end)

    lexical_weight = map_get(ranking_input, :lexical_weight, ranking_default.lexical_weight)

    {lexical_weight, errors} =
      validate_non_negative_number(
        lexical_weight,
        path ++ [:ranking, :lexical_weight],
        errors,
        ranking_default.lexical_weight
      )

    semantic_weight = map_get(ranking_input, :semantic_weight, ranking_default.semantic_weight)

    {semantic_weight, errors} =
      validate_non_negative_number(
        semantic_weight,
        path ++ [:ranking, :semantic_weight],
        errors,
        ranking_default.semantic_weight
      )

    fallback = map_get(normalized, :fallback, @defaults.retrieval.fallback)

    {fallback, errors} =
      validate_fallback(fallback, path ++ [:fallback], errors, @defaults.retrieval.fallback)

    {%{
       limit: limit,
       ranking: %{lexical_weight: lexical_weight, semantic_weight: semantic_weight},
       fallback: fallback
     }, errors}
  end

  @spec validate_plugin(term(), [ConfigError.t()]) :: {map(), [ConfigError.t()]}
  defp validate_plugin(value, errors) do
    path = [:plugin]
    {input, errors} = normalize_section_map(value, path, errors)
    {normalized, unknown} = normalize_known_keys(input, @plugin_keys)

    errors =
      Enum.reduce(unknown, errors, fn key, acc ->
        [
          error(path ++ [to_path_key(key)], :unknown_field, "unknown plugin config key", key)
          | acc
        ]
      end)

    route_prefix = map_get(normalized, :route_prefix, @defaults.plugin.route_prefix)

    {route_prefix, errors} =
      validate_non_empty_string(
        route_prefix,
        path ++ [:route_prefix],
        errors,
        @defaults.plugin.route_prefix
      )

    auto_capture = map_get(normalized, :auto_capture, @defaults.plugin.auto_capture)

    {auto_capture, errors} =
      validate_boolean(
        auto_capture,
        path ++ [:auto_capture],
        errors,
        @defaults.plugin.auto_capture
      )

    capture_signal_patterns =
      map_get(normalized, :capture_signal_patterns, @defaults.plugin.capture_signal_patterns)

    {capture_signal_patterns, errors} =
      validate_string_list(
        capture_signal_patterns,
        path ++ [:capture_signal_patterns],
        errors,
        @defaults.plugin.capture_signal_patterns
      )

    {%{
       route_prefix: route_prefix,
       auto_capture: auto_capture,
       capture_signal_patterns: capture_signal_patterns
     }, errors}
  end

  @spec validate_safety(term(), [ConfigError.t()]) :: {map(), [ConfigError.t()]}
  defp validate_safety(value, errors) do
    path = [:safety]
    {input, errors} = normalize_section_map(value, path, errors)
    {normalized, unknown} = normalize_known_keys(input, @safety_keys)

    errors =
      Enum.reduce(unknown, errors, fn key, acc ->
        [
          error(path ++ [to_path_key(key)], :unknown_field, "unknown safety config key", key)
          | acc
        ]
      end)

    audit_enabled = map_get(normalized, :audit_enabled, @defaults.safety.audit_enabled)

    {audit_enabled, errors} =
      validate_boolean(
        audit_enabled,
        path ++ [:audit_enabled],
        errors,
        @defaults.safety.audit_enabled
      )

    redaction_enabled =
      map_get(normalized, :redaction_enabled, @defaults.safety.redaction_enabled)

    {redaction_enabled, errors} =
      validate_boolean(
        redaction_enabled,
        path ++ [:redaction_enabled],
        errors,
        @defaults.safety.redaction_enabled
      )

    pii_strategy = map_get(normalized, :pii_strategy, @defaults.safety.pii_strategy)

    {pii_strategy, errors} =
      validate_pii_strategy(
        pii_strategy,
        path ++ [:pii_strategy],
        errors,
        @defaults.safety.pii_strategy
      )

    {%{
       audit_enabled: audit_enabled,
       redaction_enabled: redaction_enabled,
       pii_strategy: pii_strategy
     }, errors}
  end

  @spec validate_store(term(), term(), [atom()], [ConfigError.t()], {module(), keyword()}) ::
          {{module(), keyword()}, [ConfigError.t()]}
  defp validate_store(store_value, store_opts, path, errors, fallback_store) do
    with {:ok, {store_mod, base_opts}} <- Jido.Memory.Store.normalize_store(store_value),
         true <- is_list(store_opts) do
      {{store_mod, Keyword.merge(base_opts, store_opts)}, errors}
    else
      false ->
        {fallback_store,
         [
           error(path ++ [:store_opts], :invalid_type, "must be a keyword list", store_opts)
           | errors
         ]}

      {:error, reason} ->
        {fallback_store,
         [
           error(
             path ++ [:store],
             :invalid_store,
             "invalid store declaration: #{inspect(reason)}",
             store_value
           )
           | errors
         ]}
    end
  end

  @spec validate_fallback(term(), [atom()], [ConfigError.t()], atom()) ::
          {atom(), [ConfigError.t()]}
  defp validate_fallback(value, path, errors, fallback) do
    normalized =
      case value do
        :hybrid -> :hybrid
        :lexical -> :lexical
        :semantic -> :semantic
        "hybrid" -> :hybrid
        "lexical" -> :lexical
        "semantic" -> :semantic
        _ -> nil
      end

    if normalized do
      {normalized, errors}
    else
      {fallback,
       [error(path, :invalid_value, "must be :hybrid, :lexical, or :semantic", value) | errors]}
    end
  end

  @spec validate_pii_strategy(term(), [atom()], [ConfigError.t()], atom()) ::
          {atom(), [ConfigError.t()]}
  defp validate_pii_strategy(value, path, errors, fallback) do
    normalized =
      case value do
        :mask -> :mask
        :drop -> :drop
        :allow -> :allow
        "mask" -> :mask
        "drop" -> :drop
        "allow" -> :allow
        _ -> nil
      end

    if normalized do
      {normalized, errors}
    else
      {fallback, [error(path, :invalid_value, "must be :mask, :drop, or :allow", value) | errors]}
    end
  end

  @spec validate_positive_integer(term(), [atom()], [ConfigError.t()], pos_integer()) ::
          {pos_integer(), [ConfigError.t()]}
  defp validate_positive_integer(value, _path, errors, _fallback)
       when is_integer(value) and value > 0 do
    {value, errors}
  end

  defp validate_positive_integer(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be a positive integer", value) | errors]}
  end

  @spec validate_non_negative_number(term(), [atom()], [ConfigError.t()], number()) ::
          {number(), [ConfigError.t()]}
  defp validate_non_negative_number(value, _path, errors, _fallback)
       when is_number(value) and value >= 0 do
    {value, errors}
  end

  defp validate_non_negative_number(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be a non-negative number", value) | errors]}
  end

  @spec validate_unit_number(term(), [atom()], [ConfigError.t()], number()) ::
          {number(), [ConfigError.t()]}
  defp validate_unit_number(value, _path, errors, _fallback)
       when is_number(value) and value >= 0 and value <= 1 do
    {value, errors}
  end

  defp validate_unit_number(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be between 0 and 1", value) | errors]}
  end

  @spec validate_non_empty_string(term(), [atom()], [ConfigError.t()], String.t()) ::
          {String.t(), [ConfigError.t()]}
  defp validate_non_empty_string(value, path, errors, fallback) when is_binary(value) do
    trimmed = String.trim(value)

    if trimmed == "" do
      {fallback, [error(path, :invalid_type, "must be a non-empty string", value) | errors]}
    else
      {trimmed, errors}
    end
  end

  defp validate_non_empty_string(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be a non-empty string", value) | errors]}
  end

  @spec validate_boolean(term(), [atom()], [ConfigError.t()], boolean()) ::
          {boolean(), [ConfigError.t()]}
  defp validate_boolean(value, _path, errors, _fallback) when is_boolean(value),
    do: {value, errors}

  defp validate_boolean(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be a boolean", value) | errors]}
  end

  @spec validate_string_list(term(), [atom()], [ConfigError.t()], [String.t()]) ::
          {[String.t()], [ConfigError.t()]}
  defp validate_string_list(value, path, errors, fallback) when is_list(value) do
    case Enum.find(value, fn item -> not is_binary(item) or String.trim(item) == "" end) do
      nil ->
        {Enum.map(value, &String.trim/1), errors}

      bad ->
        {fallback,
         [
           error(path, :invalid_type, "all entries must be non-empty strings", bad)
           | errors
         ]}
    end
  end

  defp validate_string_list(value, path, errors, fallback) do
    {fallback, [error(path, :invalid_type, "must be a list of strings", value) | errors]}
  end

  @spec normalize_section_map(term(), [atom()], [ConfigError.t()]) :: {map(), [ConfigError.t()]}
  defp normalize_section_map(nil, _path, errors), do: {%{}, errors}

  defp normalize_section_map(value, _path, errors) when is_map(value), do: {value, errors}

  defp normalize_section_map(value, _path, errors) when is_list(value) do
    if Keyword.keyword?(value) do
      {Map.new(value), errors}
    else
      {%{}, [error([], :invalid_type, "invalid keyword structure", value) | errors]}
    end
  end

  defp normalize_section_map(value, path, errors) do
    {%{}, [error(path, :invalid_type, "must be a map", value) | errors]}
  end

  @spec normalize_known_keys(map(), [atom()]) :: {map(), [term()]}
  defp normalize_known_keys(input, allowed_keys) when is_map(input) do
    by_string = Map.new(allowed_keys, &{Atom.to_string(&1), &1})

    Enum.reduce(input, {%{}, []}, fn {key, value}, {acc, unknown} ->
      cond do
        is_atom(key) and key in allowed_keys ->
          {Map.put(acc, key, value), unknown}

        is_binary(key) and Map.has_key?(by_string, key) ->
          {Map.put(acc, Map.fetch!(by_string, key), value), unknown}

        true ->
          {acc, [key | unknown]}
      end
    end)
  end

  @spec normalize_input(term()) :: map()
  defp normalize_input(input) when is_map(input), do: input

  defp normalize_input(input) when is_list(input) do
    if Keyword.keyword?(input), do: Map.new(input), else: %{}
  end

  defp normalize_input(_input), do: %{}

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

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  @spec to_path_key(atom() | String.t() | term()) :: atom() | String.t()
  defp to_path_key(key) when is_atom(key), do: key
  defp to_path_key(key) when is_binary(key), do: key
  defp to_path_key(key), do: inspect(key)

  @spec error([atom() | String.t()], atom(), String.t(), term()) :: ConfigError.t()
  defp error(path, code, message, value) do
    ConfigError.new(path, code, message, value)
  end
end
