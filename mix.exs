defmodule JidoMemoryOs.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/agentjido/jido_memory_os"
  @description "Tiered memory orchestration and governance layer for Jido agents"
  @jido_memory_ref "f40f588144cf2476843210f3b5f6e23d10605e6e"

  def project do
    [
      app: :jido_memory_os,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      name: "Jido MemoryOS",
      source_url: @source_url,
      homepage_url: @source_url,
      description: @description,
      package: package(),
      docs: docs(),
      dialyzer: [
        flags: [:error_handling, :underspecs, :unknown, :unmatched_returns],
        plt_add_apps: [:mix]
      ],
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
      {:jido_memory, github: "agentjido/jido_memory", ref: @jido_memory_ref},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Research Paper" => "https://arxiv.org/abs/2506.06326"
      },
      files: [
        ".formatter.exs",
        "README.md",
        "mix.exs",
        "lib",
        "docs/user",
        "docs/developer"
      ]
    ]
  end

  defp docs do
    user_guides = Path.wildcard("docs/user/[0-9][0-9]-*.md") |> Enum.sort()
    developer_guides = Path.wildcard("docs/developer/[0-9][0-9]-*.md") |> Enum.sort()

    [
      main: "readme",
      source_ref: "main",
      extras: [{"README.md", title: "Overview"}] ++ user_guides ++ developer_guides,
      groups_for_extras: [
        "User Guides": user_guides,
        "Developer Guides": developer_guides
      ],
      extra_section: "Guides",
      formatters: ["html"]
    ]
  end
end
