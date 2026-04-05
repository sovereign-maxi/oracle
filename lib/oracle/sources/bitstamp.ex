defmodule Oracle.Sources.Bitstamp do
  @moduledoc """
  Bitstamp exchange price source adapter.

  Fetches prices from Bitstamp's public REST API.

  ## Supported Pairs

  | Pair | Bitstamp Symbol | Notes |
  |------|-----------------|-------|
  | `:btc_usd` | btcusd | Lowercase format |
  | `:eth_usd` | ethusd | |
  | `:btc_eur` | btceur | |

  ## API Endpoint

  ```
  GET https://www.bitstamp.net/api/v2/ticker/btcusd/
  Response: {"last": "104523.45", "high": "105000", "low": "103000", ...}
  ```

  ## Rate Limits

  Bitstamp allows 8000 requests per 10 minutes.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://www.bitstamp.net"
  @timeout_ms 10_000

  @impl true
  def name, do: :bitstamp

  @impl true
  def fetch_price(:btc_usdt), do: fetch_price(:btc_usd)
  def fetch_price(:eth_usdt), do: fetch_price(:eth_usd)

  def fetch_price(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v2/ticker/#{symbol}/") do
      {:ok, %{"last" => price}} ->
        safe_decimal(price)

      {:ok, %{"status" => "error", "reason" => reason}} ->
        {:error, {:api_error, reason}}

      {:ok, %{"error" => error}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches detailed ticker information including OHLC data.
  """
  @spec fetch_ticker(Oracle.Sources.Source.pair()) :: {:ok, map()} | {:error, term()}
  def fetch_ticker(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v2/ticker/#{symbol}/") do
      {:ok, ticker} when is_map(ticker) and not is_map_key(ticker, "error") ->
        with {:ok, last} <- safe_decimal(ticker["last"]),
             {:ok, high} <- safe_decimal(ticker["high"]),
             {:ok, low} <- safe_decimal(ticker["low"]),
             {:ok, open} <- safe_decimal(ticker["open"]),
             {:ok, volume} <- safe_decimal(ticker["volume"]),
             {:ok, vwap} <- safe_decimal(ticker["vwap"]),
             {:ok, bid} <- safe_decimal(ticker["bid"]),
             {:ok, ask} <- safe_decimal(ticker["ask"]),
             {:ok, timestamp} <- safe_timestamp(ticker["timestamp"]) do
          {:ok,
           %{
             last: last,
             high: high,
             low: low,
             open: open,
             volume: volume,
             vwap: vwap,
             bid: bid,
             ask: ask,
             timestamp: timestamp
           }}
        else
          {:error, reason} -> {:error, {:invalid_response, reason}}
        end

      {:ok, %{"error" => error}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches hourly ticker for OHLC analysis.
  """
  @spec fetch_ticker_hourly(Oracle.Sources.Source.pair()) :: {:ok, map()} | {:error, term()}
  def fetch_ticker_hourly(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v2/ticker_hour/#{symbol}/") do
      {:ok, ticker} when is_map(ticker) and not is_map_key(ticker, "error") ->
        with {:ok, last} <- safe_decimal(ticker["last"]),
             {:ok, high} <- safe_decimal(ticker["high"]),
             {:ok, low} <- safe_decimal(ticker["low"]),
             {:ok, open} <- safe_decimal(ticker["open"]),
             {:ok, volume} <- safe_decimal(ticker["volume"]),
             {:ok, vwap} <- safe_decimal(ticker["vwap"]) do
          {:ok,
           %{
             last: last,
             high: high,
             low: low,
             open: open,
             volume: volume,
             vwap: vwap
           }}
        else
          {:error, reason} -> {:error, {:invalid_response, reason}}
        end

      {:ok, %{"error" => error}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp pair_to_symbol(:btc_usd), do: "btcusd"
  defp pair_to_symbol(:eth_usd), do: "ethusd"
  defp pair_to_symbol(:btc_eur), do: "btceur"
  defp pair_to_symbol(:eth_eur), do: "etheur"
  defp pair_to_symbol(:btc_gbp), do: "btcgbp"
  defp pair_to_symbol(:xrp_usd), do: "xrpusd"
  defp pair_to_symbol(:ltc_usd), do: "ltcusd"

  defp pair_to_symbol(pair) do
    pair |> Atom.to_string() |> String.downcase() |> String.replace("_", "")
  end

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

  defp safe_timestamp(nil), do: {:error, :missing_timestamp}

  defp safe_timestamp(value) when is_binary(value) do
    case Integer.parse(value) do
      {unix_ts, ""} ->
        case DateTime.from_unix(unix_ts) do
          {:ok, dt} -> {:ok, dt}
          {:error, reason} -> {:error, {:invalid_timestamp, reason}}
        end

      _ ->
        {:error, {:invalid_timestamp, value}}
    end
  end

  defp safe_timestamp(value) when is_integer(value) do
    case DateTime.from_unix(value) do
      {:ok, dt} -> {:ok, dt}
      {:error, reason} -> {:error, {:invalid_timestamp, reason}}
    end
  end

  defp safe_timestamp(value), do: {:error, {:invalid_timestamp, value}}

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
