require Jido.MemoryOS.Actions.Forget
require Jido.MemoryOS.Actions.Remember
require Jido.MemoryOS.Actions.Retrieve

defmodule Jido.MemoryOS.Plugin do
  @moduledoc """
  MemoryOS plugin entrypoint for Jido agents.

  Phase 1 exposes explicit action routes and config/state placeholders.
  """

  alias Jido.MemoryOS.Actions.{Forget, Remember, Retrieve}

  @state_schema Zoi.object(%{
                  config: Zoi.map() |> Zoi.default(%{})
                })

  @config_schema Zoi.object(%{
                   config: Zoi.map() |> Zoi.default(%{}),
                   auto_capture: Zoi.boolean() |> Zoi.default(true),
                   capture_signal_patterns:
                     Zoi.list(Zoi.string())
                     |> Zoi.default(["ai.react.query", "ai.llm.response", "ai.tool.result"])
                 })

  use Jido.Plugin,
    name: "memory_os",
    state_key: :__memory_os__,
    actions: [Remember, Retrieve, Forget],
    schema: @state_schema,
    config_schema: @config_schema,
    singleton: true,
    description: "MemoryOS plugin exposing tiered memory actions",
    capabilities: [:memory_os]

  @impl Jido.Plugin
  def mount(_agent, config) do
    {:ok, %{config: map_get(config, :config, %{})}}
  end

  @impl Jido.Plugin
  def signal_routes(_config) do
    [
      {"remember", Remember},
      {"retrieve", Retrieve},
      {"forget", Forget}
    ]
  end

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
