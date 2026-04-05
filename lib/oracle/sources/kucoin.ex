defmodule Oracle.Sources.KuCoin do
  @moduledoc """
  KuCoin exchange price source adapter.

  Fetches prices from KuCoin's public REST API.

  ## Supported Pairs

  | Pair | KuCoin Symbol | Notes |
  |------|---------------|-------|
  | `:xmr_btc` | XMR-BTC | Native pair |

  ## API Endpoint

  ```
  GET https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=XMR-BTC
  Response: {"code": "200000", "data": {"price": "0.00234", ...}}
  ```

  ## Rate Limits

  KuCoin allows 30 requests per second for public endpoints.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://api.kucoin.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :kucoin

  @impl true
  def fetch_price(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v1/market/orderbook/level1?symbol=#{symbol}") do
      {:ok, %{"code" => "200000", "data" => %{"price" => price}}} ->
        safe_decimal(price)

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _} ->
        {:error, {:invalid_response, "unexpected response format"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Private ---

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

  defp pair_to_symbol(:xmr_btc), do: "XMR-BTC"

  defp pair_to_symbol(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.replace("_", "-")
  end

  defp http_get(path) do
    url = @base_url <> path

    case http_client().get(url, [], recv_timeout: @timeout_ms) do
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
