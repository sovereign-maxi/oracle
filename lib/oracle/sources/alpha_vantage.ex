defmodule Oracle.Sources.AlphaVantage do
  @moduledoc """
  Alpha Vantage `GLOBAL_QUOTE` price source.

  REST-poll oracle for US equities. The Watchdog reads via
  `fetch_price/1` on its normal tick cadence; each call is a
  single HTTP GET against Alpha Vantage's public endpoint.

  ## Endpoint

      GET https://www.alphavantage.co/query
          ?function=GLOBAL_QUOTE
          &symbol=<SYMBOL>
          &entitlement=realtime          # premium tier only
          &apikey=<KEY>

  Response body:

      {
        "Global Quote": {
          "01. symbol": "MSTR",
          "05. price": "329.20",
          ...
        }
      }

  ## Identity model

  Alpha Vantage authenticates on API key alone (credit card at
  signup, no KYC / address proof / utility bill). HTTP transport
  routes through whatever the caller wires as
  `config :oracle, :http_client` — callers that need to hide their
  IP can wire that adapter through a proxy.

  ## Rate limits

  Premium tier: 750 req/min = 12.5 QPS. One underlying polled at
  500 ms burns 2 QPS, so ~25 underlyings fit comfortably under
  the cap. The Watchdog owns the poll cadence; this module makes
  no assumptions.

  Pair naming follows the stack convention (`:mstr_usd`); the
  request is keyed by the raw ticker (`"MSTR"`).
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://www.alphavantage.co/query"
  @timeout_ms 5_000

  @impl true
  def name, do: :alpha_vantage

  @impl true
  @spec fetch_price(atom()) :: {:ok, Decimal.t()} | {:error, atom()}
  def fetch_price(pair) when is_atom(pair) do
    with {:ok, symbol} <- pair_to_symbol(pair),
         {:ok, api_key} <- api_key(),
         {:ok, body} <- do_get(quote_url(symbol, api_key)) do
      parse_price(body)
    end
  end

  @doc """
  Returns the last ~100 daily bars for `pair`, ordered oldest →
  newest. Backed by `TIME_SERIES_DAILY` — free-tier endpoint.

  Sub-minute intraday requires the premium tier + `entitlement=realtime`
  on `TIME_SERIES_INTRADAY`; once the venue is on a premium key,
  flip the URL builder to `intraday_url/2` and the parser to
  `"Time Series (1min)"`.
  """
  @spec fetch_daily(atom()) ::
          {:ok,
           [
             %{
               ts: DateTime.t(),
               open: Decimal.t(),
               high: Decimal.t(),
               low: Decimal.t(),
               close: Decimal.t(),
               price: Decimal.t(),
               volume: integer()
             }
           ]}
          | {:error, atom()}
  def fetch_daily(pair) when is_atom(pair) do
    with {:ok, symbol} <- pair_to_symbol(pair),
         {:ok, api_key} <- api_key(),
         {:ok, body} <- do_get(daily_url(symbol, api_key)) do
      parse_series(body, "Time Series (Daily)")
    end
  end

  @doc """
  Returns the last ~100 1-minute intraday bars for `pair`, oldest →
  newest. Backed by `TIME_SERIES_INTRADAY` with the realtime
  entitlement — premium-only endpoint. Newest bar is the current
  in-progress minute.
  """
  @spec fetch_intraday_1min(atom()) ::
          {:ok,
           [
             %{
               ts: DateTime.t(),
               open: Decimal.t(),
               high: Decimal.t(),
               low: Decimal.t(),
               close: Decimal.t(),
               price: Decimal.t(),
               volume: integer()
             }
           ]}
          | {:error, atom()}
  def fetch_intraday_1min(pair) when is_atom(pair) do
    with {:ok, symbol} <- pair_to_symbol(pair),
         {:ok, api_key} <- api_key(),
         {:ok, body} <- do_get(intraday_url(symbol, api_key)) do
      parse_series(body, "Time Series (1min)")
    end
  end

  # --- Internal ---

  defp pair_to_symbol(pair) do
    symbol =
      pair
      |> Atom.to_string()
      |> String.replace_suffix("_usd", "")
      |> String.upcase()

    if symbol == "" or String.contains?(symbol, "_") do
      {:error, :invalid_pair}
    else
      {:ok, symbol}
    end
  end

  defp api_key do
    case Application.get_env(:oracle, :alpha_vantage, [])[:api_key] do
      key when is_binary(key) and key != "" -> {:ok, key}
      _ -> {:error, :missing_api_key}
    end
  end

  defp http_client do
    Application.get_env(:oracle, :http_client, HTTPoison)
  end

  defp quote_url(symbol, api_key) do
    @base_url <>
      "?function=GLOBAL_QUOTE" <>
      "&symbol=" <>
      URI.encode(symbol) <>
      "&entitlement=realtime" <>
      "&apikey=" <> URI.encode(api_key)
  end

  defp intraday_url(symbol, api_key) do
    @base_url <>
      "?function=TIME_SERIES_INTRADAY" <>
      "&interval=1min" <>
      "&outputsize=compact" <>
      "&symbol=" <> URI.encode(symbol) <>
      "&entitlement=realtime" <>
      "&apikey=" <> URI.encode(api_key)
  end

  # Free-tier fallback (kept for callers without a premium key).
  defp daily_url(symbol, api_key) do
    @base_url <>
      "?function=TIME_SERIES_DAILY" <>
      "&outputsize=compact" <>
      "&symbol=" <>
      URI.encode(symbol) <>
      "&apikey=" <> URI.encode(api_key)
  end

  defp do_get(url) do
    case http_client().get(url, [], recv_timeout: @timeout_ms) do
      {:ok, %{status_code: 200, body: body}} -> {:ok, body}
      {:ok, %{status_code: code}} -> {:error, {:http_error, code}}
      {:error, %{reason: reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_price(body) do
    with {:ok, %{"Global Quote" => q}} <- Jason.decode(body),
         price when is_binary(price) <- q["05. price"],
         {:ok, decimal} <- decimal_parse(price) do
      {:ok, decimal}
    else
      {:ok, %{"Note" => _}} -> {:error, :rate_limited}
      {:ok, %{"Information" => _}} -> {:error, :rate_limited}
      {:ok, %{"Error Message" => _}} -> {:error, :unknown_symbol}
      {:ok, %{"Global Quote" => empty}} when map_size(empty) == 0 -> {:error, :no_quote}
      {:error, %Jason.DecodeError{}} -> {:error, :bad_response}
      _ -> {:error, :bad_response}
    end
  end

  defp decimal_parse(str) do
    case Decimal.parse(str) do
      {%Decimal{} = d, ""} -> {:ok, d}
      _ -> {:error, :bad_price}
    end
  end

  # Alpha Vantage returns bar series as
  #   %{"Time Series (Daily)" => %{"2026-07-17" => %{"4. close" => "329.20", ...}}}
  # keyed by timestamp string. Normalise into `{ts, price}` tuples
  # sorted oldest-first.
  defp parse_series(body, series_key) do
    with {:ok, decoded} <- Jason.decode(body),
         {:ok, series} <- take_series(decoded, series_key),
         bars when is_list(bars) <- Enum.map(series, &parse_bar/1),
         [] <- Enum.filter(bars, &match?({:error, _}, &1)) do
      sorted =
        bars
        |> Enum.map(fn {:ok, bar} -> bar end)
        |> Enum.sort_by(& &1.ts, DateTime)

      {:ok, sorted}
    else
      {:error, _} = err -> err
      [_ | _] -> {:error, :bad_response}
    end
  end

  # AV error/throttle envelopes come back with a top-level Note /
  # Information / Error Message key instead of the expected series
  # map. Intercept here so callers see a clean atom.
  defp take_series(%{} = decoded, series_key) do
    cond do
      Map.has_key?(decoded, "Note") -> {:error, :rate_limited}
      Map.has_key?(decoded, "Information") -> {:error, :rate_limited}
      Map.has_key?(decoded, "Error Message") -> {:error, :unknown_symbol}
      match?(%{} = _, Map.get(decoded, series_key)) -> {:ok, Map.fetch!(decoded, series_key)}
      true -> {:error, :bad_response}
    end
  end

  defp parse_bar(
         {ts_str,
          %{
            "1. open" => open_s,
            "2. high" => high_s,
            "3. low" => low_s,
            "4. close" => close_s,
            "5. volume" => volume_s
          }}
       )
       when is_binary(ts_str) do
    with {:ok, ts} <- parse_ts(ts_str),
         {:ok, open} <- decimal_parse(open_s),
         {:ok, high} <- decimal_parse(high_s),
         {:ok, low} <- decimal_parse(low_s),
         {:ok, close} <- decimal_parse(close_s),
         {volume, ""} <- Integer.parse(volume_s) do
      {:ok, %{ts: ts, open: open, high: high, low: low, close: close, price: close, volume: volume}}
    else
      _ -> {:error, :bad_bar}
    end
  end

  defp parse_bar(_), do: {:error, :bad_bar}

  # Alpha Vantage timestamps come as "YYYY-MM-DD" (daily) or
  # "YYYY-MM-DD HH:MM:SS" (intraday), both US/Eastern. Treat as
  # naive → UTC. Fine for ordering + display; not used for anything
  # that needs strict TZ correctness.
  defp parse_ts(str) do
    normalized =
      if String.contains?(str, " "), do: String.replace(str, " ", "T"), else: str <> "T00:00:00"

    case NaiveDateTime.from_iso8601(normalized) do
      {:ok, naive} -> {:ok, DateTime.from_naive!(naive, "Etc/UTC")}
      _ -> {:error, :bad_ts}
    end
  end
end
