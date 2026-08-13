defmodule Oracle.Sources.Source do
  @moduledoc """
  Behaviour for price data sources.

  Exchange adapters implement this behaviour to provide a consistent
  interface for fetching price data.

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
