defmodule Oracle.MixProject do
  use Mix.Project

  def project do
    [
      app: :oracle,
      version: "0.2.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),

      # Docs
      name: "Oracle",
      docs: [
        main: "Oracle",
        extras: ["README.md"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:decimal, "~> 3.0"},
      {:gun, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:mint_web_socket, "~> 1.0"},
      {:telemetry, "~> 1.2"},

      # Dev/Test
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:faker, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      test: ["test --color"],
      lint: ["credo --strict", "dialyzer"]
    ]
  end
end
