defmodule Oracle.Sources.Gemini do
  @moduledoc """
  Gemini exchange price source adapter.

  Fetches prices from Gemini's public REST API.

  ## Supported Pairs

  | Pair | Gemini Symbol | Notes |
  |------|---------------|-------|
  | `:btc_usd` | btcusd | Lowercase format |
  | `:eth_usd` | ethusd | |
  | `:eth_btc` | ethbtc | |

  ## API Endpoint

  ```
  GET https://api.gemini.com/v1/pubticker/btcusd
  Response: {"last": "104523.45", "bid": "104520", "ask": "104525", ...}
  ```

  ## Rate Limits

  Gemini allows 120 requests per minute for public endpoints.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://api.gemini.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :gemini

  @impl true
  def fetch_price(:btc_usdt), do: fetch_price(:btc_usd)
  def fetch_price(:eth_usdt), do: fetch_price(:eth_usd)

  def fetch_price(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/v1/pubticker/#{symbol}") do
      {:ok, %{"last" => price}} ->
        safe_decimal(price)

      {:ok, %{"result" => "error", "reason" => reason}} ->
        {:error, {:api_error, reason}}

      {:ok, %{"message" => message}} ->
        {:error, {:api_error, message}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches detailed ticker information including bid/ask spread.
  """
  @spec fetch_ticker(Oracle.Sources.Source.pair()) :: {:ok, map()} | {:error, term()}
  def fetch_ticker(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/v1/pubticker/#{symbol}") do
      {:ok, ticker} when is_map(ticker) and is_map_key(ticker, "last") ->
        with {:ok, last} <- safe_decimal(ticker["last"]),
             {:ok, bid} <- safe_decimal(ticker["bid"]),
             {:ok, ask} <- safe_decimal(ticker["ask"]) do
          {:ok,
           %{
             last: last,
             bid: bid,
             ask: ask,
             volume: parse_volume(ticker["volume"])
           }}
        else
          {:error, reason} -> {:error, {:invalid_response, reason}}
        end

      {:ok, %{"result" => "error", "reason" => reason}} ->
        {:error, {:api_error, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches the V2 ticker which includes more detailed volume info.
  """
  @spec fetch_ticker_v2(Oracle.Sources.Source.pair()) :: {:ok, map()} | {:error, term()}
  def fetch_ticker_v2(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/v2/ticker/#{symbol}") do
      {:ok, ticker} when is_map(ticker) and is_map_key(ticker, "close") ->
        with {:ok, open} <- safe_decimal(ticker["open"]),
             {:ok, high} <- safe_decimal(ticker["high"]),
             {:ok, low} <- safe_decimal(ticker["low"]),
             {:ok, close} <- safe_decimal(ticker["close"]),
             {:ok, bid} <- safe_decimal(ticker["bid"]),
             {:ok, ask} <- safe_decimal(ticker["ask"]) do
          {:ok,
           %{
             open: open,
             high: high,
             low: low,
             close: close,
             bid: bid,
             ask: ask,
             changes: ticker["changes"]
           }}
        else
          {:error, reason} -> {:error, {:invalid_response, reason}}
        end

      {:ok, %{"result" => "error", "reason" => reason}} ->
        {:error, {:api_error, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets available trading pairs from Gemini.
  """
  @spec list_symbols() :: {:ok, [String.t()]} | {:error, term()}
  def list_symbols do
    http_get("/v1/symbols")
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp pair_to_symbol(:btc_usd), do: "btcusd"
  defp pair_to_symbol(:eth_usd), do: "ethusd"
  defp pair_to_symbol(:eth_btc), do: "ethbtc"
  defp pair_to_symbol(:btc_eur), do: "btceur"
  defp pair_to_symbol(:btc_gbp), do: "btcgbp"
  defp pair_to_symbol(:ltc_usd), do: "ltcusd"
  defp pair_to_symbol(:ltc_btc), do: "ltcbtc"
  defp pair_to_symbol(:zec_usd), do: "zecusd"

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

  defp parse_volume(nil), do: %{}

  defp parse_volume(%{"BTC" => btc, "USD" => usd}) when not is_nil(btc) and not is_nil(usd) do
    with {:ok, base} <- safe_decimal(btc),
         {:ok, quote_val} <- safe_decimal(usd) do
      %{base: base, quote: quote_val}
    else
      _ -> %{}
    end
  end

  defp parse_volume(%{"ETH" => eth, "USD" => usd}) when not is_nil(eth) and not is_nil(usd) do
    with {:ok, base} <- safe_decimal(eth),
         {:ok, quote_val} <- safe_decimal(usd) do
      %{base: base, quote: quote_val}
    else
      _ -> %{}
    end
  end

  defp parse_volume(volume) when is_map(volume) do
    # Filter out non-numeric values (like "timestamp") and safely convert
    volume
    |> Enum.filter(fn {_k, v} -> is_binary(v) or is_number(v) end)
    |> Enum.reject(fn {k, _v} -> k == "timestamp" end)
    |> Map.new(fn {k, v} ->
      case safe_decimal(v) do
        {:ok, d} -> {k, d}
        _ -> {k, nil}
      end
    end)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  defp parse_volume(_), do: %{}

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
