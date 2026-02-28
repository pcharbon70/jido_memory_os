defmodule Jido.MemoryOS.Workers.RetrievalWorker do
  @moduledoc """
  Retrieval control-plane worker.

  Executes manager retrieval jobs in a dedicated process so callers can offload
  non-blocking retrieval requests.
  """

  use GenServer

  alias Jido.MemoryOS.MemoryManager

  @type state :: %{
          manager: GenServer.server(),
          completed_jobs: non_neg_integer()
        }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Schedules one asynchronous retrieval job.
  """
  @spec retrieve(
          GenServer.server(),
          map() | struct(),
          map() | keyword() | Jido.Memory.Query.t(),
          keyword()
        ) :: :ok
  def retrieve(server \\ __MODULE__, target, query, opts \\ []) do
    GenServer.cast(server, {:retrieve, target, query, opts})
  end

  @impl true
  def init(opts) do
    {:ok, %{manager: Keyword.get(opts, :manager, MemoryManager), completed_jobs: 0}}
  end

  @impl true
  def handle_cast({:retrieve, target, query, opts}, state) do
    manager = state.manager

    Task.start(fn ->
      _ = MemoryManager.retrieve(target, query, Keyword.put(opts, :server, manager))
      :ok
    end)

    {:noreply, %{state | completed_jobs: state.completed_jobs + 1}}
  end
end
