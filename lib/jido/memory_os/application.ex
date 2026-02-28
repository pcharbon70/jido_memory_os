defmodule Jido.MemoryOS.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    with :ok <- Jido.MemoryOS.Compatibility.validate_runtime_contract(),
         {:ok, _config} <- Jido.MemoryOS.Config.validate(app_config()) do
      children = [
        {Task.Supervisor, name: Jido.MemoryOS.ManagerWorkers},
        {DynamicSupervisor, name: Jido.MemoryOS.MaintenanceSupervisor, strategy: :one_for_one},
        {Jido.MemoryOS.MemoryManager, app_config: app_config()},
        {Jido.MemoryOS.Workers.RetrievalWorker,
         name: Jido.MemoryOS.Workers.RetrievalWorker, manager: Jido.MemoryOS.MemoryManager},
        {Jido.MemoryOS.Workers.ConsolidationWorker,
         name: Jido.MemoryOS.Workers.ConsolidationWorker, manager: Jido.MemoryOS.MemoryManager},
        {Jido.MemoryOS.Workers.PruneWorker,
         name: Jido.MemoryOS.Workers.PruneWorker, manager: Jido.MemoryOS.MemoryManager}
      ]

      Supervisor.start_link(children, strategy: :one_for_one, name: Jido.MemoryOS.Supervisor)
    end
  end

  defp app_config, do: Application.get_env(:jido_memory_os, Jido.MemoryOS.Config, %{})
end
