defmodule Oracle.Sources.Streams.Binance do
  @moduledoc """
  Binance WebSocket streaming adapter.

  Binance encodes subscriptions in the URL using combined streams.

  ## WebSocket Endpoint

      wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/btcusdt@trade

  ## Supported Feeds

  - `:ticker` - 24hr mini ticker
  - `:trades` - Real-time trade executions
  - `:liquidations` - Forced liquidation orders (futures only)

  ## Liquidations live on the futures endpoint

  `@forceOrder` streams exist only on `fstream.binance.com`. A channel
  list containing `:liquidations` is therefore routed to the futures
  base URL — and must not be mixed with spot feeds (`:ticker`,
  `:book_ticker`, `:trades`) on the same connection, since those would
  then deliver futures prices. Subscribe liquidations on their own
  connection.
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{Liquidation, Ticker, Trade}

  @ws_base_spot "wss://stream.binance.com:9443/stream"
  @ws_base_futures "wss://fstream.binance.com/stream"

  @impl true
  def name, do: :binance

  @impl true
  def ws_url(channels) do
    streams = Enum.map_join(channels, "/", &channel_to_stream/1)
    "#{ws_base(channels)}?streams=#{streams}"
  end

  @impl true
  def subscribe_messages(_channels), do: []

  @impl true
  def unsubscribe_messages(_channels), do: []

  @impl true
  def parse_message(%{"stream" => stream, "data" => data}) do
    cond do
      String.ends_with?(stream, "@miniTicker") -> parse_ticker(data)
      String.ends_with?(stream, "@bookTicker") -> parse_book_ticker(data)
      String.ends_with?(stream, "@trade") -> parse_trade(data)
      String.ends_with?(stream, "@forceOrder") -> parse_liquidation(data)
      true -> :ignore
    end
  end

  def parse_message(%{"result" => nil}), do: :ignore
  def parse_message(%{"ping" => _}), do: :ping
  def parse_message(_), do: :ignore

  @impl true
  def ping_config, do: nil

  @impl true
  def supported_feeds, do: [:ticker, :book_ticker, :trades, :liquidations]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  # `@forceOrder` exists only on the futures endpoint; spot streams only
  # on the spot endpoint. Mixing them on one connection cannot work.
  defp ws_base(channels) do
    liquidation? = fn %{feed: feed} -> feed == :liquidations end

    cond do
      Enum.all?(channels, liquidation?) ->
        @ws_base_futures

      Enum.any?(channels, liquidation?) ->
        raise ArgumentError,
              ":liquidations channels use the Binance futures endpoint and must " <>
                "be subscribed on their own connection (got a mixed channel list)"

      true ->
        @ws_base_spot
    end
  end

  defp channel_to_stream(%{feed: :ticker, pair: pair}) do
    "#{pair_to_symbol(pair)}@miniTicker"
  end

  defp channel_to_stream(%{feed: :book_ticker, pair: pair}) do
    "#{pair_to_symbol(pair)}@bookTicker"
  end

  defp channel_to_stream(%{feed: :trades, pair: pair}) do
    "#{pair_to_symbol(pair)}@trade"
  end

  defp channel_to_stream(%{feed: :liquidations, pair: pair}) do
    "#{pair_to_symbol(pair)}@forceOrder"
  end

  defp parse_ticker(data) do
    with {:ok, price} <- safe_decimal(data["c"]),
         {:ok, high} <- safe_decimal(data["h"]),
         {:ok, low} <- safe_decimal(data["l"]),
         {:ok, volume} <- safe_decimal(data["v"]) do
      {:ok,
       [
         %Ticker{
           source: :binance,
           pair: symbol_to_pair(data["s"]),
           price: price,
           bid: nil,
           ask: nil,
           volume_24h: volume,
           change_24h: nil,
           high_24h: high,
           low_24h: low,
           timestamp: event_time(data["E"])
         }
       ]}
    else
      _ -> {:error, :invalid_ticker_data}
    end
  end

  # `@bookTicker` — best bid/ask top-of-book, fires on every change.
  # No event timestamp in the payload (SPOT feed); wall-clock is the
  # only signal we have. Price = mid (bid+ask)/2 so median aggregation
  # across sources is comparing like-for-like.
  defp parse_book_ticker(%{"s" => sym} = data) do
    with {:ok, bid} <- safe_decimal(data["b"]),
         {:ok, ask} <- safe_decimal(data["a"]) do
      mid = bid |> Decimal.add(ask) |> Decimal.div(2)

      {:ok,
       [
         %Ticker{
           source: :binance,
           pair: symbol_to_pair(sym),
           price: mid,
           bid: bid,
           ask: ask,
           volume_24h: nil,
           change_24h: nil,
           high_24h: nil,
           low_24h: nil,
           timestamp: DateTime.utc_now()
         }
       ]}
    else
      _ -> {:error, :invalid_book_ticker}
    end
  end

  defp parse_book_ticker(_), do: {:error, :invalid_book_ticker}

  defp parse_trade(data) do
    with {:ok, price} <- safe_decimal(data["p"]),
         {:ok, qty} <- safe_decimal(data["q"]) do
      side = if data["m"], do: :sell, else: :buy

      {:ok,
       [
         %Trade{
           source: :binance,
           pair: symbol_to_pair(data["s"]),
           trade_id: to_string(data["t"]),
           price: price,
           quantity: qty,
           side: side,
           timestamp: event_time(data["T"])
         }
       ]}
    else
      _ -> {:error, :invalid_trade_data}
    end
  end

  defp parse_liquidation(%{"o" => order}) do
    # `ap` is the average fill price; the order price `p` can be "0" on
    # market-style liquidation orders and must never be emitted.
    with {:ok, price} <- positive_decimal(order["ap"]),
         {:ok, qty} <- safe_decimal(order["q"]) do
      side = if order["S"] == "BUY", do: :buy, else: :sell

      {:ok,
       [
         %Liquidation{
           source: :binance,
           pair: symbol_to_pair(order["s"]),
           side: side,
           price: price,
           quantity: qty,
           timestamp: event_time(order["T"])
         }
       ]}
    else
      _ -> {:error, :invalid_liquidation_data}
    end
  end

  defp parse_liquidation(_), do: :ignore

  defp pair_to_symbol(pair) do
    pair |> Atom.to_string() |> String.downcase() |> String.replace("_", "")
  end

  # Map the wire symbol back to pair atoms (same convention as the REST
  # Binance adapter). Unknown symbols fall back to a downcased atom.
  defp symbol_to_pair("BTCUSDT"), do: :btc_usdt
  defp symbol_to_pair("ETHUSDT"), do: :eth_usdt
  defp symbol_to_pair("BTCEUR"), do: :btc_eur
  defp symbol_to_pair("ETHBTC"), do: :eth_btc
  defp symbol_to_pair("PAXGUSDT"), do: :xau_usd
  defp symbol_to_pair("MSTRBUSDT"), do: :mstrbusdt

  defp symbol_to_pair(symbol) when is_binary(symbol) do
    String.to_existing_atom(String.downcase(symbol))
  rescue
    ArgumentError -> :unknown
  end

  defp symbol_to_pair(_), do: :unknown

  defp event_time(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, dt} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp event_time(_), do: DateTime.utc_now()

  defp safe_decimal(nil), do: {:error, :nil_value}
  defp safe_decimal(""), do: {:error, :empty_string}

  defp safe_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> {:error, {:invalid_decimal, value}}
    end
  end

  defp safe_decimal(value) when is_number(value), do: {:ok, Decimal.new("#{value}")}
  defp safe_decimal(_), do: {:error, :invalid_value}

  defp positive_decimal(value) do
    case safe_decimal(value) do
      {:ok, decimal} ->
        if Decimal.positive?(decimal), do: {:ok, decimal}, else: {:error, :non_positive}

      error ->
        error
    end
  end
end
