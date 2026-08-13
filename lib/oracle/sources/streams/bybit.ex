defmodule Oracle.Sources.Streams.Bybit do
  @moduledoc """
  Bybit WebSocket streaming adapter.

  Uses a single endpoint with JSON subscribe/unsubscribe messages.

  ## WebSocket Endpoint

      wss://stream.bybit.com/v5/public/linear

  ## Supported Feeds

  - `:ticker` - Real-time ticker data
  - `:trades` - Trade executions
  - `:book` - Order book snapshots and deltas
  - `:liquidations` - Forced liquidations
  - `:funding_rate` - Perpetual futures funding rates

  ## Ticker frames are deltas

  v5 linear tickers push a full snapshot first and delta updates after —
  a delta carries only changed fields, so `lastPrice` is often absent.
  Frames without a usable price are dropped here; a partial ticker with
  a nil price is never emitted downstream.
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}

  @ws_url "wss://stream.bybit.com/v5/public/linear"

  @impl true
  def name, do: :bybit

  @impl true
  def ws_url(_channels), do: @ws_url

  @impl true
  def subscribe_messages(channels) do
    topics = Enum.map(channels, &channel_to_topic/1)

    [%{"op" => "subscribe", "args" => topics}]
  end

  @impl true
  def unsubscribe_messages(channels) do
    topics = Enum.map(channels, &channel_to_topic/1)

    [%{"op" => "unsubscribe", "args" => topics}]
  end

  @impl true
  def parse_message(%{"topic" => topic, "type" => type, "data" => data} = msg) do
    cond do
      String.starts_with?(topic, "tickers.") ->
        # v5 carries the event time at the frame top level, not in `data`
        parse_ticker(data, msg["ts"])

      String.starts_with?(topic, "publicTrade.") ->
        parse_trades(data)

      String.starts_with?(topic, "orderbook.") and type == "snapshot" ->
        parse_book_snapshot(data, topic)

      String.starts_with?(topic, "orderbook.") ->
        parse_book_delta(data, topic)

      String.starts_with?(topic, "liquidation.") ->
        parse_liquidation(data)

      String.starts_with?(topic, "kline.") ->
        :ignore

      true ->
        :ignore
    end
  end

  def parse_message(%{"op" => "pong"}), do: :ping

  def parse_message(%{"op" => "subscribe", "success" => false} = msg),
    do: {:error, {:subscribe_error, msg}}

  def parse_message(%{"op" => "subscribe"}), do: :ignore
  def parse_message(%{"success" => true}), do: :ignore
  def parse_message(_), do: :ignore

  @impl true
  def ping_config do
    {%{"op" => "ping"}, 20_000}
  end

  @impl true
  def supported_feeds, do: [:ticker, :trades, :book, :liquidations, :funding_rate]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp channel_to_topic(%{feed: :ticker, pair: pair}), do: "tickers.#{pair_to_symbol(pair)}"
  defp channel_to_topic(%{feed: :trades, pair: pair}), do: "publicTrade.#{pair_to_symbol(pair)}"
  defp channel_to_topic(%{feed: :book, pair: pair}), do: "orderbook.50.#{pair_to_symbol(pair)}"

  defp channel_to_topic(%{feed: :liquidations, pair: pair}),
    do: "liquidation.#{pair_to_symbol(pair)}"

  defp channel_to_topic(%{feed: :funding_rate, pair: pair}), do: "tickers.#{pair_to_symbol(pair)}"

  defp parse_ticker(data, frame_ts) when is_map(data) do
    pair = symbol_to_pair(data["symbol"])

    ticker =
      case positive_decimal(data["lastPrice"]) do
        {:ok, price} ->
          %Ticker{
            source: :bybit,
            pair: pair,
            price: price,
            bid: parse_decimal(data["bid1Price"]),
            ask: parse_decimal(data["ask1Price"]),
            volume_24h: parse_decimal(data["volume24h"]),
            change_24h: parse_decimal(data["price24hPcnt"]),
            high_24h: parse_decimal(data["highPrice24h"]),
            low_24h: parse_decimal(data["lowPrice24h"]),
            timestamp: event_time(frame_ts)
          }

        {:error, _} ->
          # Delta tickers omit unchanged fields — no price signal, no tick.
          nil
      end

    result = if ticker, do: [ticker], else: []

    result =
      case parse_decimal(data["fundingRate"]) do
        %Decimal{} = rate ->
          [
            %FundingRate{
              source: :bybit,
              pair: pair,
              rate: rate,
              next_funding_time: event_time(data["nextFundingTime"]),
              timestamp: event_time(frame_ts)
            }
            | result
          ]

        _ ->
          result
      end

    case result do
      [] -> :ignore
      structs -> {:ok, structs}
    end
  end

  defp parse_ticker(_, _), do: :ignore

  defp parse_trades(data) when is_list(data) do
    trades = Enum.flat_map(data, &build_trade/1)
    {:ok, trades}
  end

  defp parse_trades(_), do: :ignore

  defp build_trade(item) do
    with {:ok, price} <- safe_decimal(item["p"]),
         {:ok, qty} <- safe_decimal(item["v"]) do
      side = if item["S"] == "Buy", do: :buy, else: :sell

      [
        %Trade{
          source: :bybit,
          pair: symbol_to_pair(item["s"]),
          trade_id: item["i"],
          price: price,
          quantity: qty,
          side: side,
          timestamp: event_time(item["T"])
        }
      ]
    else
      _ -> []
    end
  end

  defp parse_book_snapshot(data, topic) do
    pair = topic_to_pair(topic)
    bids = parse_levels(data["b"])
    asks = parse_levels(data["a"])

    {:ok,
     [
       %BookSnapshot{
         source: :bybit,
         pair: pair,
         bids: bids,
         asks: asks,
         sequence: data["seq"],
         timestamp: event_time(data["ts"])
       }
     ]}
  end

  defp parse_book_delta(data, topic) do
    pair = topic_to_pair(topic)
    bids = parse_levels(data["b"])
    asks = parse_levels(data["a"])

    {:ok,
     [
       %BookDelta{
         source: :bybit,
         pair: pair,
         bids: bids,
         asks: asks,
         first_sequence: data["seq"],
         last_sequence: data["seq"],
         timestamp: event_time(data["ts"])
       }
     ]}
  end

  defp parse_liquidation(data) when is_map(data) do
    with {:ok, price} <- safe_decimal(data["price"]),
         {:ok, qty} <- safe_decimal(data["size"]) do
      side = if data["side"] == "Buy", do: :buy, else: :sell

      {:ok,
       [
         %Liquidation{
           source: :bybit,
           pair: symbol_to_pair(data["symbol"]),
           side: side,
           price: price,
           quantity: qty,
           timestamp: event_time(data["updatedTime"])
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
    pair |> Atom.to_string() |> String.upcase() |> String.replace("_", "")
  end

  # Map the wire symbol back to pair atoms. Unknown symbols fall back
  # to a downcased atom.
  defp symbol_to_pair("BTCUSDT"), do: :btc_usdt
  defp symbol_to_pair("ETHUSDT"), do: :eth_usdt

  defp symbol_to_pair(symbol) when is_binary(symbol) do
    String.to_existing_atom(String.downcase(symbol))
  rescue
    ArgumentError -> :unknown
  end

  defp symbol_to_pair(_), do: :unknown

  defp topic_to_pair(topic) do
    topic
    |> String.split(".")
    |> List.last()
    |> symbol_to_pair()
  end

  defp parse_decimal(nil), do: nil

  defp parse_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp parse_decimal(value) when is_number(value), do: Decimal.new("#{value}")
  defp parse_decimal(_), do: nil

  defp positive_decimal(value) do
    case parse_decimal(value) do
      %Decimal{} = decimal ->
        if Decimal.positive?(decimal), do: {:ok, decimal}, else: {:error, :non_positive}

      _ ->
        {:error, :invalid_decimal}
    end
  end

  defp event_time(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, dt} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp event_time(ms) when is_binary(ms) do
    case Integer.parse(ms) do
      {int, ""} -> event_time(int)
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
