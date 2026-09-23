defmodule Oracle.Sources.Binance do
  @moduledoc """
  Binance exchange price source adapter.

  Fetches prices from Binance's public REST API.

  ## Supported Pairs

  | Pair | Binance Symbol | Notes |
  |------|----------------|-------|
  | `:btc_usd` | BTCUSDT | Uses USDT (stablecoin) |
  | `:btc_usdt` | BTCUSDT | Native pair |
  | `:eth_usd` | ETHUSDT | Uses USDT |
  | `:eth_usdt` | ETHUSDT | Native pair |

  ## API Endpoint

  ```
  GET https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT
  Response: {"symbol": "BTCUSDT", "price": "104523.45"}
  ```

  ## Rate Limits

  Binance allows 1200 requests per minute for the ticker endpoint.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://api.binance.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :binance

  @impl true
  def fetch_price(:btc_usd), do: fetch_price(:btc_usdt)
  def fetch_price(:eth_usd), do: fetch_price(:eth_usdt)

  def fetch_price(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v3/ticker/price?symbol=#{symbol}") do
      {:ok, %{"price" => price}} ->
        parse_price(price)

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _other} ->
        {:error, :invalid_response_shape}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches OHLCV klines for `pair` at the given `interval`.

  ## Parameters

  - `pair` — one of the mapped atoms (e.g. `:mstrbusdt`)
  - `opts`
    - `:interval` — string like `"1m"`, `"5m"`, `"1h"` (default `"1m"`)
    - `:limit` — max 1000 (default 100) — ignored when a range is given
    - `:start_ms` — inclusive lower bound in unix milliseconds
    - `:end_ms` — inclusive upper bound in unix milliseconds

  Passing BOTH `:start_ms` and `:end_ms` fetches the range and pages
  internally in 1000-kline windows. Passing neither returns the most
  recent `limit` klines.

  ## Response

      [
        %{
          ts: ~U[2026-07-30 06:39:00Z],
          open: Decimal.new("95.30"),
          high: Decimal.new("95.40"),
          low: Decimal.new("95.30"),
          close: Decimal.new("95.40"),
          volume: Decimal.new("20.5")
        },
        ...
      ]

  Any malformed kline fails the whole call — a gap in the series is
  never returned as if valid.

  Oldest → newest ordering. Public endpoint, no auth required.
  """
  @spec fetch_klines(atom(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def fetch_klines(pair, opts \\ []) do
    interval = Keyword.get(opts, :interval, "1m")
    start_ms = Keyword.get(opts, :start_ms)
    end_ms = Keyword.get(opts, :end_ms)

    if is_integer(start_ms) and is_integer(end_ms) do
      fetch_klines_range(pair, interval, start_ms, end_ms)
    else
      limit = Keyword.get(opts, :limit, 100)
      fetch_klines_recent(pair, interval, limit)
    end
  end

  defp fetch_klines_recent(pair, interval, limit) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v3/klines?symbol=#{symbol}&interval=#{interval}&limit=#{limit}") do
      {:ok, klines} when is_list(klines) ->
        parse_klines(klines)

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _other} ->
        {:error, :invalid_response_shape}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_klines_range(_pair, _interval, start_ms, end_ms) when start_ms > end_ms, do: {:ok, []}

  defp fetch_klines_range(pair, interval, start_ms, end_ms) do
    case interval_ms(interval) do
      nil -> {:error, {:unsupported_interval, interval}}
      step_ms -> page_range(pair_to_symbol(pair), interval, step_ms, start_ms, end_ms)
    end
  end

  defp page_range(symbol, interval, step_ms, start_ms, end_ms) do
    window_ms = 1000 * step_ms

    start_ms
    |> page_windows(step_ms, window_ms, end_ms)
    |> Enum.reduce_while({:ok, []}, &accumulate_page(&1, &2, symbol, interval))
  end

  defp page_windows(start_ms, step_ms, window_ms, end_ms) do
    Stream.unfold(start_ms, fn
      s when s > end_ms -> nil
      s -> {{s, min(s + window_ms - step_ms, end_ms)}, s + window_ms}
    end)
  end

  defp accumulate_page({s, e}, {:ok, acc}, symbol, interval) do
    case fetch_klines_page(symbol, interval, s, e) do
      {:ok, klines} -> {:cont, {:ok, acc ++ klines}}
      {:error, _} = err -> {:halt, err}
    end
  end

  defp fetch_klines_page(symbol, interval, start_ms, end_ms) do
    path =
      "/api/v3/klines?symbol=#{symbol}&interval=#{interval}&startTime=#{start_ms}&endTime=#{end_ms}&limit=1000"

    case http_get(path) do
      {:ok, klines} when is_list(klines) ->
        parse_klines(klines)

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _other} ->
        {:error, :invalid_response_shape}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_klines(klines) do
    klines
    |> Enum.map(&parse_kline/1)
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, kline}, {:ok, acc} -> {:cont, {:ok, [kline | acc]}}
      {:error, _}, _acc -> {:halt, {:error, :invalid_kline_data}}
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      error -> error
    end
  end

  defp interval_ms("1m"), do: 60_000
  defp interval_ms("3m"), do: 180_000
  defp interval_ms("5m"), do: 300_000
  defp interval_ms("15m"), do: 900_000
  defp interval_ms("30m"), do: 1_800_000
  defp interval_ms("1h"), do: 3_600_000
  defp interval_ms("2h"), do: 7_200_000
  defp interval_ms("4h"), do: 14_400_000
  defp interval_ms("6h"), do: 21_600_000
  defp interval_ms("8h"), do: 28_800_000
  defp interval_ms("12h"), do: 43_200_000
  defp interval_ms("1d"), do: 86_400_000
  defp interval_ms(_), do: nil

  @doc """
  Fetches rolling 24-hour stats for `pair`.

  Returns:

      %{
        price: Decimal.new("95.44"),
        change: Decimal.new("-2.35"),
        change_pct: Decimal.new("-2.403"),
        open_24h: Decimal.new("97.79"),
        high_24h: Decimal.new("100.07"),
        low_24h: Decimal.new("92.28"),
        volume_24h: Decimal.new("14775.25")
      }

  Any malformed field fails the whole call — a partial or zero-filled
  stats map is never returned as if valid.

  Public endpoint, no auth. Cheap to call per underlying every few seconds.
  """
  @spec fetch_24h_stats(atom()) :: {:ok, map()} | {:error, term()}
  def fetch_24h_stats(pair) do
    symbol = pair_to_symbol(pair)

    case http_get("/api/v3/ticker/24hr?symbol=#{symbol}") do
      {:ok, %{"lastPrice" => _} = resp} ->
        with {:ok, price} <- parse_price(resp["lastPrice"]),
             {:ok, change} <- parse_signed(resp["priceChange"]),
             {:ok, change_pct} <- parse_signed(resp["priceChangePercent"]),
             {:ok, open} <- parse_price(resp["openPrice"]),
             {:ok, high} <- parse_price(resp["highPrice"]),
             {:ok, low} <- parse_price(resp["lowPrice"]),
             {:ok, volume} <- parse_volume(resp["volume"]) do
          {:ok,
           %{
             price: price,
             change: change,
             change_pct: change_pct,
             open_24h: open,
             high_24h: high,
             low_24h: low,
             volume_24h: volume
           }}
        else
          _ -> {:error, :invalid_stats_data}
        end

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _other} ->
        {:error, :invalid_response_shape}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches prices for multiple pairs in a single request.

  More efficient than calling `fetch_price/1` multiple times.
  """
  @spec fetch_prices([Oracle.Sources.Source.pair()]) ::
          {:ok, %{Oracle.Sources.Source.pair() => Decimal.t()}} | {:error, term()}
  def fetch_prices(pairs) do
    symbols = Enum.map(pairs, &pair_to_symbol/1)
    symbols_param = Jason.encode!(symbols)

    case http_get("/api/v3/ticker/price?symbols=#{URI.encode(symbols_param)}") do
      {:ok, prices} when is_list(prices) ->
        price_map = Enum.reduce(prices, %{}, &extract_price_entry/2)
        {:ok, price_map}

      {:ok, %{"code" => code, "msg" => msg}} ->
        {:error, {:api_error, code, msg}}

      {:ok, _other} ->
        {:error, :invalid_response_shape}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp extract_price_entry(%{"symbol" => symbol, "price" => price}, acc) do
    with {:ok, pair} <- symbol_to_pair(symbol),
         {:ok, decimal} <- parse_price(price) do
      Map.put(acc, pair, decimal)
    else
      _ -> acc
    end
  end

  defp extract_price_entry(_, acc), do: acc

  defp pair_to_symbol(:btc_usdt), do: "BTCUSDT"
  defp pair_to_symbol(:eth_usdt), do: "ETHUSDT"
  defp pair_to_symbol(:btc_eur), do: "BTCEUR"
  defp pair_to_symbol(:eth_btc), do: "ETHBTC"
  defp pair_to_symbol(:xau_usd), do: "PAXGUSDT"
  # Tokenized-stock spot products (Binance's `B`-suffix convention).
  defp pair_to_symbol(:mstrbusdt), do: "MSTRBUSDT"

  defp pair_to_symbol(pair) do
    symbol = pair |> Atom.to_string() |> String.upcase() |> String.replace("_", "")

    # Sanitize: only alphanumeric characters in symbol to prevent URL injection
    if Regex.match?(~r/^[A-Z0-9]+$/, symbol) do
      symbol
    else
      raise ArgumentError, "invalid pair: #{inspect(pair)}"
    end
  end

  defp symbol_to_pair("BTCUSDT"), do: {:ok, :btc_usdt}
  defp symbol_to_pair("ETHUSDT"), do: {:ok, :eth_usdt}
  defp symbol_to_pair("BTCEUR"), do: {:ok, :btc_eur}
  defp symbol_to_pair("ETHBTC"), do: {:ok, :eth_btc}
  defp symbol_to_pair("PAXGUSDT"), do: {:ok, :xau_usd}
  defp symbol_to_pair("MSTRBUSDT"), do: {:ok, :mstrbusdt}
  defp symbol_to_pair(_), do: :error

  defp parse_price(nil), do: {:error, {:invalid_price, nil}}
  defp parse_price(""), do: {:error, {:invalid_price, ""}}

  defp parse_price(price) when is_binary(price) do
    case Decimal.parse(price) do
      {decimal, ""} ->
        if Decimal.positive?(decimal) do
          {:ok, decimal}
        else
          {:error, {:invalid_price, :non_positive}}
        end

      _ ->
        {:error, {:invalid_price, price}}
    end
  end

  defp parse_price(price) when is_integer(price) and price > 0 do
    {:ok, Decimal.new(price)}
  end

  defp parse_price(price) when is_float(price) and price > 0 do
    {:ok, Decimal.from_float(price)}
  end

  defp parse_price(price), do: {:error, {:invalid_price, price}}

  # Binance kline array shape (index-based):
  #   [ open_ms, open_str, high_str, low_str, close_str, volume_str, ... ]
  defp parse_kline([open_ms, o, h, l, c, v | _rest]) do
    with {:ok, open} <- parse_price(o),
         {:ok, high} <- parse_price(h),
         {:ok, low} <- parse_price(l),
         {:ok, close} <- parse_price(c),
         {:ok, volume} <- parse_volume(v) do
      {:ok,
       %{
         ts: DateTime.from_unix!(open_ms, :millisecond),
         open: open,
         high: high,
         low: low,
         close: close,
         volume: volume
       }}
    else
      _ -> {:error, :invalid_kline_data}
    end
  end

  defp parse_kline(_), do: {:error, :invalid_kline_data}

  # Signed fields (24h change) can legitimately be negative — only the
  # shape is validated.
  defp parse_signed(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> {:error, {:invalid_decimal, value}}
    end
  end

  defp parse_signed(value) when is_number(value), do: {:ok, Decimal.new("#{value}")}
  defp parse_signed(_), do: {:error, :invalid_value}

  # Base volume is fractional (e.g. BTC) — keep full precision.
  defp parse_volume(str) when is_binary(str) do
    case Decimal.parse(str) do
      {decimal, ""} ->
        if Decimal.negative?(decimal), do: {:error, :invalid_volume}, else: {:ok, decimal}

      _ ->
        {:error, {:invalid_volume, str}}
    end
  end

  defp parse_volume(value) when is_number(value) and value >= 0,
    do: {:ok, Decimal.new("#{value}")}

  defp parse_volume(_), do: {:error, :invalid_volume}

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
