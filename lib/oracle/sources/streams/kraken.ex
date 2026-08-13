defmodule Oracle.Sources.Streams.Kraken do
  @moduledoc """
  Kraken WebSocket streaming adapter.

  Uses a single endpoint with JSON subscribe/unsubscribe messages.

  ## WebSocket Endpoint

      wss://ws.kraken.com

  ## Supported Feeds

  - `:ticker` - Real-time ticker data
  - `:trades` - Trade executions
  - `:book` - Order book snapshots and deltas
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{BookDelta, BookSnapshot, Ticker, Trade}

  @ws_url "wss://ws.kraken.com"

  @impl true
  def name, do: :kraken

  @impl true
  def ws_url(_channels), do: @ws_url

  @impl true
  def subscribe_messages(channels) do
    grouped = group_by_feed(channels)

    Enum.map(grouped, fn {feed, pairs} ->
      %{
        "event" => "subscribe",
        "pair" => Enum.map(pairs, &pair_to_kraken/1),
        "subscription" => %{"name" => feed_to_name(feed)}
      }
    end)
  end

  @impl true
  def unsubscribe_messages(channels) do
    grouped = group_by_feed(channels)

    Enum.map(grouped, fn {feed, pairs} ->
      %{
        "event" => "unsubscribe",
        "pair" => Enum.map(pairs, &pair_to_kraken/1),
        "subscription" => %{"name" => feed_to_name(feed)}
      }
    end)
  end

  @impl true
  def parse_message(%{"event" => "heartbeat"}), do: :ignore
  def parse_message(%{"event" => "pong"}), do: :ping
  def parse_message(%{"event" => "systemStatus"}), do: :ignore

  def parse_message(%{"event" => "subscriptionStatus", "status" => "error"} = msg),
    do: {:error, {:subscribe_error, msg}}

  def parse_message(%{"event" => "subscriptionStatus"}), do: :ignore

  def parse_message(msg) when is_list(msg) do
    case msg do
      [_channel_id, data, "ticker", pair] ->
        parse_ticker(data, pair)

      [_channel_id, data, "trade", pair] ->
        parse_trades(data, pair)

      [_channel_id, %{"as" => _, "bs" => _} = data, "book-10", pair] ->
        parse_book_snapshot(data, pair)

      [_channel_id, data, "book-10", pair] ->
        parse_book_delta(data, pair)

      # Two-sided updates arrive as [id, %{"a" => ...}, %{"b" => ...}, name, pair]
      [_channel_id, side_a, side_b, "book-10", pair]
      when is_map(side_a) and is_map(side_b) ->
        parse_book_delta(Map.merge(side_a, side_b), pair)

      _ ->
        :ignore
    end
  end

  def parse_message(_), do: :ignore

  @impl true
  def ping_config do
    {%{"event" => "ping"}, 30_000}
  end

  @impl true
  def supported_feeds, do: [:ticker, :trades, :book]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp parse_ticker(data, pair) when is_map(data) do
    with {:ok, price} <- safe_decimal(get_first(data["c"])),
         {:ok, bid} <- safe_decimal(get_first(data["b"])),
         {:ok, ask} <- safe_decimal(get_first(data["a"])),
         {:ok, volume} <- safe_decimal(get_at(data["v"], 1)),
         {:ok, high} <- safe_decimal(get_at(data["h"], 1)),
         {:ok, low} <- safe_decimal(get_at(data["l"], 1)) do
      {:ok,
       [
         %Ticker{
           source: :kraken,
           pair: kraken_to_pair(pair),
           price: price,
           bid: bid,
           ask: ask,
           volume_24h: volume,
           change_24h: nil,
           high_24h: high,
           low_24h: low,
           timestamp: DateTime.utc_now()
         }
       ]}
    else
      _ -> {:error, :invalid_ticker_data}
    end
  end

  defp parse_ticker(_, _), do: :ignore

  defp parse_trades(data, pair) when is_list(data) do
    trades = Enum.flat_map(data, &build_trade(&1, pair))
    {:ok, trades}
  end

  defp parse_trades(_, _), do: :ignore

  defp build_trade([price_str, vol_str, _time, side_str, _type, _misc], pair) do
    with {:ok, price} <- safe_decimal(price_str),
         {:ok, qty} <- safe_decimal(vol_str) do
      side = if side_str == "b", do: :buy, else: :sell

      [
        %Trade{
          source: :kraken,
          pair: kraken_to_pair(pair),
          trade_id: nil,
          price: price,
          quantity: qty,
          side: side,
          timestamp: DateTime.utc_now()
        }
      ]
    else
      _ -> []
    end
  end

  defp build_trade(_, _pair), do: []

  defp parse_book_snapshot(data, pair) do
    bids = parse_levels(data["bs"])
    asks = parse_levels(data["as"])

    {:ok,
     [
       %BookSnapshot{
         source: :kraken,
         pair: kraken_to_pair(pair),
         bids: bids,
         asks: asks,
         sequence: nil,
         timestamp: DateTime.utc_now()
       }
     ]}
  end

  defp parse_book_delta(data, pair) do
    bids = parse_levels(Map.get(data, "b", []))
    asks = parse_levels(Map.get(data, "a", []))

    {:ok,
     [
       %BookDelta{
         source: :kraken,
         pair: kraken_to_pair(pair),
         bids: bids,
         asks: asks,
         first_sequence: nil,
         last_sequence: nil,
         timestamp: DateTime.utc_now()
       }
     ]}
  end

  defp group_by_feed(channels) do
    Enum.group_by(channels, & &1.feed, & &1.pair)
  end

  defp feed_to_name(:ticker), do: "ticker"
  defp feed_to_name(:trades), do: "trade"
  defp feed_to_name(:book), do: "book"

  defp pair_to_kraken(:btc_usd), do: "XBT/USD"
  defp pair_to_kraken(:btc_usdt), do: "XBT/USDT"
  defp pair_to_kraken(:eth_usd), do: "ETH/USD"
  defp pair_to_kraken(:eth_btc), do: "ETH/XBT"

  defp pair_to_kraken(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.replace("_", "/")
  end

  defp kraken_to_pair(pair_str) when is_binary(pair_str) do
    pair_str
    |> String.replace("XBT", "BTC")
    |> String.replace("/", "_")
    |> String.downcase()
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :unknown
  end

  defp kraken_to_pair(_), do: :unknown

  defp parse_levels(levels) when is_list(levels) do
    Enum.flat_map(levels, fn
      [price_str, qty_str | _] ->
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

  defp get_first([val | _]), do: val
  defp get_first(_), do: nil

  defp get_at(list, idx) when is_list(list), do: Enum.at(list, idx)
  defp get_at(_, _), do: nil

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
