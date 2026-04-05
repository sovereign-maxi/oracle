defmodule Oracle.Sources.Kraken do
  @moduledoc """
  Kraken exchange price source adapter.

  Fetches prices from Kraken's public REST API.

  ## Supported Pairs

  | Pair | Kraken Symbol | Notes |
  |------|---------------|-------|
  | `:btc_usd` | XXBTZUSD | Uses XBT symbol for BTC |
  | `:eth_usd` | XETHZUSD | |
  | `:btc_usdt` | XBTUSDT | USDT pair |

  ## API Endpoint

  ```
  GET https://api.kraken.com/0/public/Ticker?pair=XBTUSD
  Response: {"error": [], "result": {"XXBTZUSD": {"c": ["104523.45", "0.123"]}}}
  ```

  The `c` field contains [price, lot_volume] of the last trade.

  ## Rate Limits

  Kraken allows 15 requests per second for public endpoints.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://api.kraken.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :kraken

  @impl true
  def fetch_price(:btc_usdt), do: fetch_with_symbol("XBTUSDT", "XBTUSDT")

  def fetch_price(pair) do
    {symbol, result_key} = pair_to_symbol(pair)
    fetch_with_symbol(symbol, result_key)
  end

  defp fetch_with_symbol(symbol, result_key) do
    case http_get("/0/public/Ticker?pair=#{symbol}") do
      {:ok, %{"error" => [], "result" => result}} ->
        # Kraken may return different keys, try both
        ticker = Map.get(result, result_key) || Map.get(result, symbol)

        case ticker do
          %{"c" => [price | _]} ->
            safe_decimal(price)

          _ ->
            {:error, {:invalid_response, "missing price data"}}
        end

      {:ok, %{"error" => [error | _]}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches detailed ticker information including bid/ask.
  """
  @spec fetch_ticker(Oracle.Sources.Source.pair()) :: {:ok, map()} | {:error, term()}
  def fetch_ticker(pair) do
    {symbol, result_key} = pair_to_symbol(pair)

    case http_get("/0/public/Ticker?pair=#{symbol}") do
      {:ok, %{"error" => [], "result" => result}} ->
        ticker = Map.get(result, result_key) || Map.get(result, symbol)
        build_ticker_map(ticker)

      {:ok, %{"error" => [error | _]}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches prices for multiple pairs in a single request.
  """
  @spec fetch_prices([Oracle.Sources.Source.pair()]) ::
          {:ok, %{Oracle.Sources.Source.pair() => Decimal.t()}} | {:error, term()}
  def fetch_prices(pairs) do
    symbols = Enum.map_join(pairs, ",", &elem(pair_to_symbol(&1), 0))

    case http_get("/0/public/Ticker?pair=#{symbols}") do
      {:ok, %{"error" => [], "result" => result}} ->
        price_map = Enum.reduce(pairs, %{}, &extract_pair_price(&1, &2, result))
        {:ok, price_map}

      {:ok, %{"error" => [error | _]}} ->
        {:error, {:api_error, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp build_ticker_map(%{
         "a" => [ask | _],
         "b" => [bid | _],
         "c" => [last | _],
         "v" => [_today_vol, vol]
       }) do
    with {:ok, ask_d} <- safe_decimal(ask),
         {:ok, bid_d} <- safe_decimal(bid),
         {:ok, last_d} <- safe_decimal(last),
         {:ok, vol_d} <- safe_decimal(vol) do
      {:ok, %{ask: ask_d, bid: bid_d, last: last_d, volume_24h: vol_d}}
    else
      {:error, reason} -> {:error, {:invalid_ticker_data, reason}}
    end
  end

  defp build_ticker_map(_), do: {:error, {:invalid_response, "missing ticker data"}}

  defp extract_pair_price(pair, acc, result) do
    {_symbol, result_key} = pair_to_symbol(pair)
    ticker = Map.get(result, result_key)

    case ticker do
      %{"c" => [price | _]} ->
        case safe_decimal(price) do
          {:ok, decimal} -> Map.put(acc, pair, decimal)
          {:error, _} -> acc
        end

      _ ->
        acc
    end
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

  # Kraken uses XBT for Bitcoin and has specific pair naming
  defp pair_to_symbol(:btc_usd), do: {"XBTUSD", "XXBTZUSD"}
  defp pair_to_symbol(:eth_usd), do: {"ETHUSD", "XETHZUSD"}
  defp pair_to_symbol(:btc_eur), do: {"XBTEUR", "XXBTZEUR"}
  defp pair_to_symbol(:eth_eur), do: {"ETHEUR", "XETHZEUR"}
  defp pair_to_symbol(:xmr_btc), do: {"XMRXBT", "XXMRXXBT"}
  defp pair_to_symbol(:xau_usd), do: {"XAUUSD", "XXAUZUSD"}
  defp pair_to_symbol(:xag_usd), do: {"XAGUSD", "XXAGZUSD"}

  defp pair_to_symbol(pair) do
    symbol = pair |> Atom.to_string() |> String.upcase() |> String.replace("_", "")
    {symbol, symbol}
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
