defmodule Jido.MemoryOS.Adapter.MemoryRuntime do
  @moduledoc """
  Stable adapter around `Jido.Memory.Runtime` with MemoryOS-specific defaults.
  """

  alias Jido.Memory.Runtime
  alias Jido.Memory.Store
  alias Jido.MemoryOS.{Config, ErrorMapping, Metadata}

  @type context :: %{
          config: Config.t(),
          tier: Config.tier(),
          namespace: String.t(),
          store: {module(), keyword()},
          runtime_opts: keyword(),
          correlation_id: String.t(),
          now: integer()
        }

  @doc """
  Writes a memory record using resolved tier namespace/store.
  """
  @spec remember(map() | struct(), map() | keyword(), keyword()) ::
          {:ok, Jido.Memory.Record.t()} | {:error, ErrorMapping.jido_error()}
  def remember(target, attrs, opts \\ []) do
    operation = :remember

    with {:ok, context} <- resolve_context(target, opts),
         {:ok, attrs} <- prepare_attrs(attrs, context, operation, opts),
         {:ok, record} <-
           call_runtime(operation, fn -> Runtime.remember(target, attrs, context.runtime_opts) end) do
      {:ok, record}
    else
      {:error, reason} -> {:error, ErrorMapping.from_reason(reason, operation)}
    end
  end

  @doc """
  Reads one memory record.
  """
  @spec get(map() | struct(), String.t(), keyword()) ::
          {:ok, Jido.Memory.Record.t()} | {:error, ErrorMapping.jido_error()}
  def get(target, id, opts \\ []) do
    operation = :get

    with {:ok, context} <- resolve_context(target, opts),
         {:ok, record} <-
           call_runtime(operation, fn -> Runtime.get(target, id, context.runtime_opts) end) do
      {:ok, record}
    else
      {:error, reason} -> {:error, ErrorMapping.from_reason(reason, operation)}
    end
  end

  @doc """
  Retrieves records using structured filters.
  """
  @spec recall(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, ErrorMapping.jido_error()}
  def recall(target, query, opts \\ []) do
    operation = :recall

    with {:ok, context} <- resolve_context(target, opts),
         {:ok, query_map} <- prepare_query(query, context),
         {:ok, records} <- call_runtime(operation, fn -> Runtime.recall(target, query_map) end) do
      {:ok, records}
    else
      {:error, reason} -> {:error, ErrorMapping.from_reason(reason, operation)}
    end
  end

  @doc """
  Deletes one memory record by id.
  """
  @spec forget(map() | struct(), String.t(), keyword()) ::
          {:ok, boolean()} | {:error, ErrorMapping.jido_error()}
  def forget(target, id, opts \\ []) do
    operation = :forget

    with {:ok, context} <- resolve_context(target, opts),
         {:ok, deleted?} <-
           call_runtime(operation, fn -> Runtime.forget(target, id, context.runtime_opts) end) do
      {:ok, deleted?}
    else
      {:error, reason} -> {:error, ErrorMapping.from_reason(reason, operation)}
    end
  end

  @doc """
  Prunes expired records in the selected store.
  """
  @spec prune(map() | struct(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, ErrorMapping.jido_error()}
  def prune(target, opts \\ []) do
    operation = :prune

    with {:ok, context} <- resolve_context(target, opts),
         {:ok, count} <-
           call_runtime(operation, fn -> Runtime.prune_expired(target, context.runtime_opts) end) do
      {:ok, count}
    else
      {:error, reason} -> {:error, ErrorMapping.from_reason(reason, operation)}
    end
  end

  @doc """
  Resolves effective runtime context including config precedence.
  """
  @spec resolve_context(map() | struct(), keyword()) :: {:ok, context()} | {:error, term()}
  def resolve_context(target, opts) do
    opts = normalize_opts(opts)
    app_config = Keyword.get(opts, :app_config, %{})
    plugin_state = explicit_plugin_state(opts) || extract_plugin_state(target)

    with {:ok, config} <- Config.resolve(opts, plugin_state, app_config),
         {:ok, tier} <- normalize_tier(Keyword.get(opts, :tier, :short)),
         {:ok, namespace} <- resolve_namespace(config, target, tier, opts),
         {:ok, store} <- resolve_store(config, tier, opts) do
      correlation_id =
        Keyword.get_lazy(opts, :correlation_id, fn ->
          "memos-" <> Integer.to_string(System.unique_integer([:positive]))
        end)

      {:ok,
       %{
         config: config,
         tier: tier,
         namespace: namespace,
         store: store,
         runtime_opts: [namespace: namespace, store: store],
         correlation_id: to_string(correlation_id),
         now: System.system_time(:millisecond)
       }}
    end
  end

  @spec resolve_namespace(Config.t(), map() | struct(), Config.tier(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  defp resolve_namespace(config, target, tier, opts) do
    agent_id =
      Keyword.get_lazy(opts, :agent_id, fn ->
        extract_agent_id(target)
      end)

    Config.tier_namespace(config, agent_id, tier, Keyword.get(opts, :namespace))
  end

  @spec resolve_store(Config.t(), Config.tier(), keyword()) ::
          {:ok, {module(), keyword()}} | {:error, term()}
  defp resolve_store(config, tier, opts) do
    explicit_store = Keyword.get(opts, :store)

    if is_nil(explicit_store) do
      Config.tier_store(config, tier)
    else
      store_opts = Keyword.get(opts, :store_opts, [])

      with {:ok, {store_mod, base_opts}} <- Store.normalize_store(explicit_store),
           true <- is_list(store_opts) do
        {:ok, {store_mod, Keyword.merge(base_opts, store_opts)}}
      else
        false -> {:error, :invalid_store_opts}
        {:error, _reason} = error -> error
      end
    end
  end

  @spec prepare_attrs(map() | keyword(), context(), atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp prepare_attrs(attrs, context, operation, opts) do
    attrs_map = normalize_map(attrs)
    mem_os_updates = normalize_map(Keyword.get(opts, :mem_os, %{}))

    mem_os_updates =
      mem_os_updates
      |> Map.put_new(:tier, context.tier)
      |> Map.put_new(:last_accessed_at, context.now)

    attrs_with_trace =
      attrs_map
      |> ensure_map_metadata()
      |> put_trace_metadata(context, operation)

    Metadata.encode_attrs(attrs_with_trace, mem_os_updates,
      previous_tier: Keyword.get(opts, :previous_tier),
      strict_transition: Keyword.get(opts, :strict_transition, true)
    )
  end

  @spec prepare_query(map() | keyword() | Jido.Memory.Query.t(), context()) ::
          {:ok, map()} | {:error, term()}
  defp prepare_query(%Jido.Memory.Query{} = query, context) do
    query_map = Map.from_struct(query)
    {:ok, Map.merge(query_map, %{namespace: context.namespace, store: context.store})}
  end

  defp prepare_query(query, context) when is_map(query) or is_list(query) do
    query_map = normalize_map(query)
    {:ok, query_map |> Map.put(:namespace, context.namespace) |> Map.put(:store, context.store)}
  end

  defp prepare_query(_query, _context), do: {:error, :invalid_query}

  @spec ensure_map_metadata(map()) :: map()
  defp ensure_map_metadata(attrs) do
    metadata = normalize_map(map_get(attrs, :metadata, %{}))
    Map.put(attrs, :metadata, metadata)
  end

  @spec put_trace_metadata(map(), context(), atom()) :: map()
  defp put_trace_metadata(attrs, context, operation) do
    metadata = normalize_map(map_get(attrs, :metadata, %{}))

    trace = %{
      "correlation_id" => context.correlation_id,
      "operation" => Atom.to_string(operation),
      "tier" => Atom.to_string(context.tier),
      "at" => context.now
    }

    Map.put(attrs, :metadata, Map.put(metadata, "mem_os_trace", trace))
  end

  @spec call_runtime(atom(), (-> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  defp call_runtime(_operation, function) do
    function.()
  rescue
    error -> {:error, {:runtime_exception, error, __STACKTRACE__}}
  end

  @spec normalize_tier(term()) :: {:ok, Config.tier()} | {:error, term()}
  defp normalize_tier(:short), do: {:ok, :short}
  defp normalize_tier(:mid), do: {:ok, :mid}
  defp normalize_tier(:long), do: {:ok, :long}
  defp normalize_tier("short"), do: {:ok, :short}
  defp normalize_tier("mid"), do: {:ok, :mid}
  defp normalize_tier("long"), do: {:ok, :long}
  defp normalize_tier(other), do: {:error, {:invalid_tier, other}}

  @spec extract_agent_id(term()) :: String.t() | nil
  defp extract_agent_id(%{id: id}) when is_binary(id), do: id
  defp extract_agent_id(%{agent: %{id: id}}) when is_binary(id), do: id
  defp extract_agent_id(_target), do: nil

  @spec extract_plugin_state(term()) :: map()
  defp extract_plugin_state(%{state: %{} = state}), do: pick_plugin_state(state)
  defp extract_plugin_state(%{} = target), do: pick_plugin_state(target)
  defp extract_plugin_state(_target), do: %{}

  @spec explicit_plugin_state(keyword()) :: map() | nil
  defp explicit_plugin_state(opts) do
    case Keyword.get(opts, :plugin_state) do
      %{} = state -> state
      _ -> nil
    end
  end

  @spec pick_plugin_state(map()) :: map()
  defp pick_plugin_state(map) do
    map_get(map, :__memory_os__, map_get(map, :memory_os, %{}))
    |> case do
      %{} = plugin_state -> plugin_state
      _ -> %{}
    end
  end

  @spec normalize_opts(term()) :: keyword()
  defp normalize_opts(opts) when is_list(opts), do: opts
  defp normalize_opts(_opts), do: []

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_value), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
