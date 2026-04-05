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
