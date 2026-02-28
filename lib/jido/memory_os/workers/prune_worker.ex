defmodule Jido.MemoryOS.Workers.PruneWorker do
  @moduledoc """
  Placeholder worker for periodic prune maintenance jobs.
  """

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, opts)

  @impl true
  def init(state), do: {:ok, state}
end
