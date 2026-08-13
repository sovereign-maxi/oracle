defmodule Oracle.Sources.TwelveData do
  @moduledoc """
  Twelve Data price source — WebSocket-primary, REST fallback.

  This façade implements the same `Oracle.Sources.Source` behaviour
  as the other REST-poll sources so callers can invoke `fetch_price/1`
  on their normal tick cadence without knowing that Twelve Data is
  push-based.

  ## Read path

  On each `fetch_price/1`:

    1. Try `Oracle.Streams.Cache.latest(:twelve_data, pair)`. If the
       runner GenServer has a fresh WebSocket tick, return it — this
       is the happy path and adds ~microseconds to a caller's cycle.
    2. If the cache is empty or stale, fall back to a REST GET
       against `/quote`. This handles cold-start (before the WS has
       delivered its first tick), server-side WS closes we haven't
       reconnected through yet, and any middlebox-induced silence.

  ## Endpoint (fallback)

      GET https://api.twelvedata.com/quote?symbol=<SYMBOL>&apikey=<KEY>

  Response:

      {
        "symbol":"MSTR",
        "name":"MicroStrategy Incorporated",
        "close":"329.20",
        "timestamp":1737494400,
        ...
      }
  """

  @behaviour Oracle.Sources.Source

  alias Oracle.Streams.Cache

  @base_url "https://api.twelvedata.com/quote"
  @timeout_ms 5_000
  @cache_max_age_ms 5_000

  @impl true
  def name, do: :twelve_data

  @impl true
  @spec fetch_price(atom()) :: {:ok, Decimal.t()} | {:error, term()}
  def fetch_price(pair) when is_atom(pair) do
    case Cache.latest(:twelve_data, pair, max_age_ms: @cache_max_age_ms) do
      {:ok, %{price: %Decimal{} = price}} ->
        {:ok, price}

      _ ->
        rest_fallback(pair)
    end
  end

  # --- Internal ---

  defp rest_fallback(pair) do
    with {:ok, symbol} <- pair_to_symbol(pair),
         {:ok, api_key} <- api_key(),
         {:ok, body} <- do_get(quote_url(symbol, api_key)) do
      parse_price(body)
    end
  end

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
    case Application.get_env(:oracle, :twelve_data, [])[:api_key] do
      key when is_binary(key) and key != "" -> {:ok, key}
      _ -> {:error, :missing_api_key}
    end
  end

  defp http_client do
    Application.get_env(:oracle, :http_client, HTTPoison)
  end

  defp quote_url(symbol, api_key) do
    @base_url <>
      "?symbol=" <>
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
    case Jason.decode(body) do
      {:ok, %{"code" => code}} when is_integer(code) and code >= 400 ->
        {:error, :vendor_error}

      {:ok, %{"status" => "error"}} ->
        {:error, :vendor_error}

      {:ok, %{"close" => close}} when is_binary(close) ->
        decimal_parse(close)

      {:ok, %{"price" => price}} when is_binary(price) ->
        decimal_parse(price)

      {:error, %Jason.DecodeError{}} ->
        {:error, :bad_response}

      _ ->
        {:error, :bad_response}
    end
  end

  defp decimal_parse(str) do
    case Decimal.parse(str) do
      {%Decimal{} = d, ""} -> {:ok, d}
      _ -> {:error, :bad_price}
    end
  end
end
