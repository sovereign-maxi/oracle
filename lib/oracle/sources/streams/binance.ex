defmodule Oracle.Sources.Streams.Binance do
  @moduledoc """
  Binance WebSocket streaming adapter.

  Binance encodes subscriptions in the URL using combined streams.

  ## WebSocket Endpoint

      wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/btcusdt@trade

  ## Supported Feeds

  - `:ticker` - 24hr mini ticker
  - `:trades` - Real-time trade executions
  - `:book` - Depth updates (diff)
  - `:liquidations` - Forced liquidation orders (futures)
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{BookDelta, Liquidation, Ticker, Trade}

  @ws_base "wss://stream.binance.com:9443/stream"

  @impl true
  def name, do: :binance

  @impl true
  def ws_url(channels) do
    streams = Enum.map_join(channels, "/", &channel_to_stream/1)
    "#{@ws_base}?streams=#{streams}"
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
      String.ends_with?(stream, "@depth") -> parse_depth(data)
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
  def supported_feeds, do: [:ticker, :book_ticker, :trades, :book, :liquidations]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp channel_to_stream(%{feed: :ticker, pair: pair}) do
    "#{pair_to_symbol(pair)}@miniTicker"
  end

  defp channel_to_stream(%{feed: :book_ticker, pair: pair}) do
    "#{pair_to_symbol(pair)}@bookTicker"
  end

  defp channel_to_stream(%{feed: :trades, pair: pair}) do
    "#{pair_to_symbol(pair)}@trade"
  end

  defp channel_to_stream(%{feed: :book, pair: pair}) do
    "#{pair_to_symbol(pair)}@depth@100ms"
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

  defp parse_depth(data) do
    bids = parse_levels(data["b"])
    asks = parse_levels(data["a"])

    {:ok,
     [
       %BookDelta{
         source: :binance,
         pair: symbol_to_pair(data["s"]),
         bids: bids,
         asks: asks,
         first_sequence: data["U"],
         last_sequence: data["u"],
         timestamp: event_time(data["E"])
       }
     ]}
  end

  defp parse_liquidation(%{"o" => order}) do
    with {:ok, price} <- safe_decimal(order["p"]),
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

  defp parse_levels(levels) when is_list(levels) do
    Enum.flat_map(levels, fn
      [price_str, qty_str] ->
        with {:ok, price} <- safe_decimal(price_str),
             {:ok, qty} <- safe_decimal_or_zero(qty_str) do
          [{price, qty}]
        else
          _ -> []
        end

      _ ->
        []
    end)
  end

  defp parse_levels(_), do: []

  defp pair_to_symbol(pair) do
    pair |> Atom.to_string() |> String.downcase() |> String.replace("_", "")
  end

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

  defp safe_decimal_or_zero(value) do
    case safe_decimal(value) do
      {:ok, _} = ok -> ok
      _ -> {:ok, Decimal.new(0)}
    end
  end
end
