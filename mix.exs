defmodule JidoMemoryOs.MixProject do
  use Mix.Project

  @jido_memory_ref "f40f588144cf2476843210f3b5f6e23d10605e6e"

  def project do
    [
      app: :jido_memory_os,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Jido.MemoryOS.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jido, "~> 2.0.0-rc.5"},
      {:jido_action, github: "agentjido/jido_action", branch: "main", override: true},
      {:jido_memory, github: "agentjido/jido_memory", ref: @jido_memory_ref}
    ]
  end
end
