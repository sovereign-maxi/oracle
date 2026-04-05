defmodule Oracle.Sources.Source do
  @moduledoc """
  Behaviour for price data sources.

  Exchange adapters implement this behaviour to provide a consistent
  interface for fetching price data.

  ## Implementing a Source

      defmodule MyApp.Sources.Binance do
        @behaviour Oracle.Sources.Source

        @impl true
        def name, do: :binance

        @impl true
        def fetch_price(:btc_usd) do
          # Binance uses USDT pairs
          fetch_price(:btc_usdt)
        end

        def fetch_price(:btc_usdt) do
          case HTTPoison.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT") do
            {:ok, %{status_code: 200, body: body}} ->
              %{"price" => price} = Jason.decode!(body)
              {:ok, Decimal.new(price)}

            {:ok, %{status_code: code}} ->
              {:error, {:http_error, code}}

            {:error, reason} ->
              {:error, reason}
          end
        end
      end

  ## Supported Pairs

  Common trading pairs:
  - `:btc_usd` - Bitcoin / US Dollar
  - `:btc_usdt` - Bitcoin / Tether
  - `:eth_usd` - Ethereum / US Dollar
  - `:xau_usd` - Gold / US Dollar
  - `:xag_usd` - Silver / US Dollar
  """

  @type pair :: :btc_usd | :btc_usdt | :eth_usd | :xau_usd | :xag_usd | atom()

  @doc """
  Returns the source identifier.
  """
  @callback name() :: atom()

  @doc """
  Fetches the current price for a trading pair.

  ## Returns

  - `{:ok, Decimal.t()}` - Price fetched successfully
  - `{:error, term()}` - Error fetching price
  """
  @callback fetch_price(pair()) :: {:ok, Decimal.t()} | {:error, term()}
end
