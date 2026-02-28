defmodule Jido.MemoryOS.Workers.ConsolidationWorker do
  @moduledoc """
  Placeholder worker for future consolidation jobs.
  """

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, opts)

  @impl true
  def init(state), do: {:ok, state}
end
