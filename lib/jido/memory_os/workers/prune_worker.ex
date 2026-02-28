defmodule Jido.MemoryOS.Workers.PruneWorker do
  @moduledoc """
  Maintenance control-plane worker for prune operations.
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
  Schedules one background prune request.
  """
  @spec prune(GenServer.server(), map() | struct(), keyword()) :: :ok
  def prune(server \\ __MODULE__, target, opts \\ []) do
    GenServer.cast(server, {:prune, target, opts})
  end

  @impl true
  def init(opts) do
    {:ok, %{manager: Keyword.get(opts, :manager, MemoryManager), completed_jobs: 0}}
  end

  @impl true
  def handle_cast({:prune, target, opts}, state) do
    manager = state.manager

    Task.start(fn ->
      _ = MemoryManager.prune(target, Keyword.put(opts, :server, manager))
      :ok
    end)

    {:noreply, %{state | completed_jobs: state.completed_jobs + 1}}
  end
end
