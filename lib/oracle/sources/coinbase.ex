defmodule Oracle.Sources.Coinbase do
  @moduledoc """
  Coinbase exchange price source adapter.

  Fetches prices from Coinbase's public REST API.

  ## Supported Pairs

  | Pair | Coinbase Format | Notes |
  |------|-----------------|-------|
  | `:btc_usd` | BTC-USD | Native USD pair |
  | `:eth_usd` | ETH-USD | Native USD pair |
  | `:btc_usdt` | BTC-USD | Falls back to USD |

  ## API Endpoint

  ```
  GET https://api.coinbase.com/v2/prices/BTC-USD/spot
  Response: {"data": {"base": "BTC", "currency": "USD", "amount": "104523.45"}}
  ```

  ## Rate Limits

  Coinbase public API allows 10,000 requests per hour.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://api.coinbase.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :coinbase

  @impl true
  def fetch_price(:btc_usdt), do: fetch_price(:btc_usd)
  def fetch_price(:eth_usdt), do: fetch_price(:eth_usd)

  def fetch_price(pair) do
    currency_pair = pair_to_currency_pair(pair)

    case http_get("/v2/prices/#{currency_pair}/spot") do
      {:ok, %{"data" => %{"amount" => amount}}} ->
        safe_decimal(amount)

      {:ok, %{"errors" => [%{"message" => msg} | _]}} ->
        {:error, {:api_error, msg}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches buy, sell, and spot prices for a pair.

  Returns all three price types in a single response.
  """
  @spec fetch_prices_detailed(Oracle.Sources.Source.pair()) ::
          {:ok, %{spot: Decimal.t(), buy: Decimal.t(), sell: Decimal.t()}} | {:error, term()}
  def fetch_prices_detailed(pair) do
    currency_pair = pair_to_currency_pair(pair)

    with {:ok, %{"data" => %{"amount" => spot}}} <- http_get("/v2/prices/#{currency_pair}/spot"),
         {:ok, %{"data" => %{"amount" => buy}}} <- http_get("/v2/prices/#{currency_pair}/buy"),
         {:ok, %{"data" => %{"amount" => sell}}} <- http_get("/v2/prices/#{currency_pair}/sell"),
         {:ok, spot_decimal} <- safe_decimal(spot),
         {:ok, buy_decimal} <- safe_decimal(buy),
         {:ok, sell_decimal} <- safe_decimal(sell) do
      {:ok,
       %{
         spot: spot_decimal,
         buy: buy_decimal,
         sell: sell_decimal
       }}
    else
      {:ok, %{"errors" => [%{"message" => msg} | _]}} ->
        {:error, {:api_error, msg}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp safe_decimal(nil), do: {:error, {:invalid_price, nil}}
  defp safe_decimal(""), do: {:error, {:invalid_price, ""}}

  defp safe_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} ->
        if Decimal.positive?(decimal) do
          {:ok, decimal}
        else
          {:error, {:invalid_price, :non_positive}}
        end

      _ ->
        {:error, {:invalid_price, value}}
    end
  end

  defp safe_decimal(value) when is_number(value) and value > 0 do
    {:ok, Decimal.new(value)}
  end

  defp safe_decimal(value), do: {:error, {:invalid_price, value}}

  defp pair_to_currency_pair(:btc_usd), do: "BTC-USD"
  defp pair_to_currency_pair(:eth_usd), do: "ETH-USD"
  defp pair_to_currency_pair(:btc_eur), do: "BTC-EUR"
  defp pair_to_currency_pair(:eth_eur), do: "ETH-EUR"
  defp pair_to_currency_pair(:btc_gbp), do: "BTC-GBP"

  defp pair_to_currency_pair(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.replace("_", "-")
  end

  defp http_get(path) do
    url = @base_url <> path

    headers = [
      {"Accept", "application/json"},
      {"CB-VERSION", "2024-01-01"}
    ]

    case http_client().get(url, headers, recv_timeout: @timeout_ms) do
      {:ok, %{status_code: 200, body: body}} ->
        Jason.decode(body)

      {:ok, %{status_code: code, body: body}} ->
        case Jason.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          _ -> {:error, {:http_error, code}}
        end

      {:error, %{reason: reason}} ->
        {:error, {:connection_error, reason}}

      {:error, reason} ->
        {:error, {:request_failed, reason}}
    end
  end

  defp http_client do
    Application.get_env(:oracle, :http_client, HTTPoison)
  end
end
