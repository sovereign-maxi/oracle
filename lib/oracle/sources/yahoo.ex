defmodule Oracle.Sources.Yahoo do
  @moduledoc """
  Yahoo Finance price source adapter.

  Fetches commodity, index, and forex prices from Yahoo Finance's
  chart API. Used for instruments not available on crypto exchanges:
  Brent crude, S&P 500, DXY, and VIX.

  ## Supported Pairs

  | Pair | Yahoo Symbol | Notes |
  |------|-------------|-------|
  | `:brent_usd` | BZ=F | ICE Brent Crude Futures |
  | `:spx_usd` | ^GSPC | S&P 500 Index |
  | `:dxy_usd` | DX-Y.NYB | US Dollar Index |
  | `:vix_usd` | ^VIX | CBOE Volatility Index |
  | `:xau_usd` | GC=F | Gold Futures (backup) |

  ## API Endpoint

  ```
  GET https://query1.finance.yahoo.com/v8/finance/chart/BZ=F?interval=1d&range=1d
  Response: {"chart":{"result":[{"meta":{"regularMarketPrice":105.53}}]}}
  ```

  ## Rate Limits

  Yahoo Finance is unofficial and rate-limits aggressively.
  Requires User-Agent header. Poll no faster than every 30 seconds.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://query1.finance.yahoo.com"
  @timeout_ms 10_000

  @impl true
  def name, do: :yahoo

  @impl true
  def fetch_price(pair) do
    symbol = pair_to_symbol(pair)

    if symbol do
      fetch_yahoo_price(symbol)
    else
      {:error, {:unsupported_pair, pair}}
    end
  end

  # --- Private ---

  defp fetch_yahoo_price(symbol) do
    encoded = URI.encode(symbol)
    url = "#{@base_url}/v8/finance/chart/#{encoded}?interval=1d&range=1d"

    headers = [
      {"User-Agent", "Mozilla/5.0 (compatible; OracleBot/1.0)"},
      {"Accept", "application/json"}
    ]

    case http_client().get(url, headers, recv_timeout: @timeout_ms) do
      {:ok, %{status_code: 200, body: body}} ->
        parse_yahoo_response(body)

      {:ok, %{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, %{reason: reason}} ->
        {:error, {:connection_error, reason}}

      {:error, reason} ->
        {:error, {:request_failed, reason}}
    end
  end

  defp parse_yahoo_response(body) do
    case Jason.decode(body) do
      {:ok, %{"chart" => %{"result" => [%{"meta" => meta} | _]}}} ->
        price = Map.get(meta, "regularMarketPrice")

        if is_number(price) and price > 0 do
          {:ok, Decimal.new("#{price}")}
        else
          {:error, {:invalid_price, price}}
        end

      {:ok, %{"chart" => %{"error" => error}}} ->
        {:error, {:api_error, error}}

      {:error, _} ->
        {:error, :json_decode_error}
    end
  end

  defp pair_to_symbol(:brent_usd), do: "BZ=F"
  defp pair_to_symbol(:spx_usd), do: "^GSPC"
  defp pair_to_symbol(:dxy_usd), do: "DX-Y.NYB"
  defp pair_to_symbol(:vix_usd), do: "^VIX"
  defp pair_to_symbol(:xau_usd), do: "GC=F"
  defp pair_to_symbol(:wti_usd), do: "CL=F"
  defp pair_to_symbol(_), do: nil

  defp http_client do
    Application.get_env(:oracle, :http_client, HTTPoison)
  end
end
