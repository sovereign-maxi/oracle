defmodule Oracle.Sources.CoinbaseStream do
  @moduledoc """
  Coinbase WebSocket streaming adapter.

  Uses a single endpoint with JSON subscribe/unsubscribe messages.

  ## WebSocket Endpoint

      wss://ws-feed.exchange.coinbase.com

  ## Supported Feeds

  - `:ticker` - Real-time price ticker
  - `:trades` - Match/trade events
  - `:book` - Level2 order book updates
  """

  @behaviour Oracle.Sources.StreamSource

  alias Oracle.Feeds.{BookDelta, BookSnapshot, Ticker, Trade}

  @ws_url "wss://ws-feed.exchange.coinbase.com"

  @impl true
  def name, do: :coinbase

  @impl true
  def ws_url(_channels), do: @ws_url

  @impl true
  def subscribe_messages(channels) do
    grouped = group_by_feed(channels)

    Enum.map(grouped, fn {feed, pairs} ->
      %{
        "type" => "subscribe",
        "product_ids" => Enum.map(pairs, &pair_to_product/1),
        "channels" => [feed_to_channel(feed)]
      }
    end)
  end

  @impl true
  def unsubscribe_messages(channels) do
    grouped = group_by_feed(channels)

    Enum.map(grouped, fn {feed, pairs} ->
      %{
        "type" => "unsubscribe",
        "product_ids" => Enum.map(pairs, &pair_to_product/1),
        "channels" => [feed_to_channel(feed)]
      }
    end)
  end

  @impl true
  def parse_message(%{"type" => "ticker"} = msg) do
    with {:ok, price} <- safe_decimal(msg["price"]),
         {:ok, bid} <- safe_decimal(msg["best_bid"]),
         {:ok, ask} <- safe_decimal(msg["best_ask"]),
         {:ok, volume} <- safe_decimal(msg["volume_24h"]),
         {:ok, low} <- safe_decimal(msg["low_24h"]),
         {:ok, high} <- safe_decimal(msg["high_24h"]) do
      {:ok,
       [
         %Ticker{
           source: :coinbase,
           pair: product_to_pair(msg["product_id"]),
           price: price,
           bid: bid,
           ask: ask,
           volume_24h: volume,
           change_24h: nil,
           high_24h: high,
           low_24h: low,
           timestamp: parse_time(msg["time"])
         }
       ]}
    else
      _ -> {:error, :invalid_ticker_data}
    end
  end

  def parse_message(%{"type" => "match"} = msg) do
    parse_match(msg)
  end

  def parse_message(%{"type" => "last_match"} = msg) do
    parse_match(msg)
  end

  def parse_message(%{"type" => "snapshot"} = msg) do
    bids = parse_levels(msg["bids"])
    asks = parse_levels(msg["asks"])

    {:ok,
     [
       %BookSnapshot{
         source: :coinbase,
         pair: product_to_pair(msg["product_id"]),
         bids: bids,
         asks: asks,
         sequence: nil,
         timestamp: DateTime.utc_now()
       }
     ]}
  end

  def parse_message(%{"type" => "l2update"} = msg) do
    {bids, asks} = partition_changes(msg["changes"])

    {:ok,
     [
       %BookDelta{
         source: :coinbase,
         pair: product_to_pair(msg["product_id"]),
         bids: bids,
         asks: asks,
         first_sequence: nil,
         last_sequence: nil,
         timestamp: parse_time(msg["time"])
       }
     ]}
  end

  def parse_message(%{"type" => "subscriptions"}), do: :ignore
  def parse_message(%{"type" => "heartbeat"}), do: :ignore
  def parse_message(%{"type" => "error"}), do: :ignore
  def parse_message(_), do: :ignore

  @impl true
  def ping_config, do: nil

  @impl true
  def supported_feeds, do: [:ticker, :trades, :book]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp parse_match(msg) do
    with {:ok, price} <- safe_decimal(msg["price"]),
         {:ok, qty} <- safe_decimal(msg["size"]) do
      side = if msg["side"] == "buy", do: :buy, else: :sell

      {:ok,
       [
         %Trade{
           source: :coinbase,
           pair: product_to_pair(msg["product_id"]),
           trade_id: to_string(msg["trade_id"]),
           price: price,
           quantity: qty,
           side: side,
           timestamp: parse_time(msg["time"])
         }
       ]}
    else
      _ -> {:error, :invalid_trade_data}
    end
  end

  defp group_by_feed(channels) do
    Enum.group_by(channels, & &1.feed, & &1.pair)
  end

  defp feed_to_channel(:ticker), do: "ticker"
  defp feed_to_channel(:trades), do: "matches"
  defp feed_to_channel(:book), do: "level2"

  defp pair_to_product(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.replace("_", "-")
  end

  defp product_to_pair(product) when is_binary(product) do
    product
    |> String.downcase()
    |> String.replace("-", "_")
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :unknown
  end

  defp product_to_pair(_), do: :unknown

  defp partition_changes(changes) when is_list(changes) do
    Enum.reduce(changes, {[], []}, fn
      ["buy", price_str, size_str], {bids, asks} ->
        case parse_level(price_str, size_str) do
          {:ok, level} -> {[level | bids], asks}
          _ -> {bids, asks}
        end

      ["sell", price_str, size_str], {bids, asks} ->
        case parse_level(price_str, size_str) do
          {:ok, level} -> {bids, [level | asks]}
          _ -> {bids, asks}
        end

      _, acc ->
        acc
    end)
  end

  defp partition_changes(_), do: {[], []}

  defp parse_level(price_str, qty_str) do
    with {:ok, price} <- safe_decimal(price_str),
         {:ok, qty} <- safe_decimal_or_zero(qty_str) do
      {:ok, {price, qty}}
    end
  end

  defp parse_levels(levels) when is_list(levels) do
    Enum.flat_map(levels, fn
      [price_str, qty_str] ->
        case parse_level(price_str, qty_str) do
          {:ok, level} -> [level]
          _ -> []
        end

      _ ->
        []
    end)
  end

  defp parse_levels(_), do: []

  defp parse_time(time_str) when is_binary(time_str) do
    case DateTime.from_iso8601(time_str) do
      {:ok, dt, _} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp parse_time(_), do: DateTime.utc_now()

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
