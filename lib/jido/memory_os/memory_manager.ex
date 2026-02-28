defmodule Jido.MemoryOS.MemoryManager do
  @moduledoc """
  Phase 1 orchestration process for MemoryOS operations.
  """

  use GenServer

  alias Jido.MemoryOS.Adapter.MemoryRuntime
  alias Jido.MemoryOS.Config

  @type state :: %{app_config: map()}

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Writes one memory record.
  """
  @spec remember(map() | struct(), map() | keyword(), keyword()) ::
          {:ok, Jido.Memory.Record.t()} | {:error, term()}
  def remember(target, attrs, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:remember, target, attrs, runtime_opts})
  end

  @doc """
  Retrieves records by query.
  """
  @spec retrieve(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, [Jido.Memory.Record.t()]} | {:error, term()}
  def retrieve(target, query, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:retrieve, target, query, runtime_opts})
  end

  @doc """
  Deletes one record by id.
  """
  @spec forget(map() | struct(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def forget(target, id, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:forget, target, id, runtime_opts})
  end

  @doc """
  Prunes expired records from the selected store.
  """
  @spec prune(map() | struct(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune(target, opts \\ []) do
    {server, runtime_opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:prune, target, runtime_opts})
  end

  @doc """
  Phase 1 placeholder for lifecycle consolidation.
  """
  @spec consolidate(map() | struct(), keyword()) :: {:ok, map()}
  def consolidate(_target, _opts \\ []), do: {:ok, %{status: :queued, phase: 1}}

  @doc """
  Returns retrieval diagnostics payload.
  """
  @spec explain_retrieval(map() | struct(), map() | keyword() | Jido.Memory.Query.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def explain_retrieval(target, query, opts \\ []) do
    with {:ok, records} <- retrieve(target, query, opts),
         {:ok, config} <- current_config(Keyword.get(opts, :server, __MODULE__)) do
      {:ok,
       %{
         query: query,
         result_count: length(records),
         retrieval: config.retrieval,
         tier: Keyword.get(opts, :tier, :short)
       }}
    end
  end

  @doc """
  Returns currently loaded app config after validation/defaulting.
  """
  @spec current_config(GenServer.server()) :: {:ok, Config.t()} | {:error, term()}
  def current_config(server \\ __MODULE__) do
    GenServer.call(server, :current_config)
  end

  @impl true
  def init(opts) do
    app_config = Keyword.get(opts, :app_config, Config.app_config())
    {:ok, %{app_config: app_config}}
  end

  @impl true
  def handle_call({:remember, target, attrs, runtime_opts}, _from, state) do
    result = MemoryRuntime.remember(target, attrs, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:retrieve, target, query, runtime_opts}, _from, state) do
    result = MemoryRuntime.recall(target, query, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:forget, target, id, runtime_opts}, _from, state) do
    result = MemoryRuntime.forget(target, id, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call({:prune, target, runtime_opts}, _from, state) do
    result = MemoryRuntime.prune(target, with_app_config(runtime_opts, state))
    {:reply, result, state}
  end

  @impl true
  def handle_call(:current_config, _from, state) do
    {:reply, Config.validate(state.app_config), state}
  end

  @spec with_app_config(keyword(), state()) :: keyword()
  defp with_app_config(runtime_opts, state) do
    Keyword.put_new(runtime_opts, :app_config, state.app_config)
  end
end
