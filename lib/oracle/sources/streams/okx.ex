defmodule Oracle.Sources.Streams.Okx do
  @moduledoc """
  OKX WebSocket streaming adapter.

  Uses a single endpoint with JSON subscribe/unsubscribe messages.

  ## WebSocket Endpoint

      wss://ws.okx.com:8443/ws/v5/public

  ## Supported Feeds

  - `:ticker` - Real-time ticker data
  - `:trades` - Trade executions
  - `:book` - Order book snapshots and deltas
  - `:liquidations` - Forced liquidations
  - `:funding_rate` - Perpetual futures funding rates
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}

  @ws_url "wss://ws.okx.com:8443/ws/v5/public"

  @impl true
  def name, do: :okx

  @impl true
  def ws_url(_channels), do: @ws_url

  @impl true
  def subscribe_messages(channels) do
    args = Enum.map(channels, &channel_to_arg/1)
    [%{"op" => "subscribe", "args" => args}]
  end

  @impl true
  def unsubscribe_messages(channels) do
    args = Enum.map(channels, &channel_to_arg/1)
    [%{"op" => "unsubscribe", "args" => args}]
  end

  @impl true
  def parse_message(%{"arg" => %{"channel" => channel}, "action" => action, "data" => data})
      when is_list(data) do
    cond do
      channel == "tickers" -> parse_tickers(data)
      channel == "trades" -> parse_trades(data)
      channel == "books5" and action == "snapshot" -> parse_book_snapshot(data)
      channel == "books5" -> parse_book_delta(data)
      channel == "liquidation-orders" -> parse_liquidations(data)
      channel == "funding-rate" -> parse_funding_rates(data)
      true -> :ignore
    end
  end

  def parse_message(%{"arg" => %{"channel" => channel}, "data" => data})
      when is_list(data) do
    cond do
      channel == "tickers" -> parse_tickers(data)
      channel == "trades" -> parse_trades(data)
      channel == "books5" -> parse_book_snapshot(data)
      channel == "liquidation-orders" -> parse_liquidations(data)
      channel == "funding-rate" -> parse_funding_rates(data)
      true -> :ignore
    end
  end

  def parse_message(%{"event" => "subscribe"}), do: :ignore
  def parse_message(%{"event" => "unsubscribe"}), do: :ignore
  def parse_message(%{"event" => "error"}), do: :ignore
  def parse_message(%{"op" => "pong"}), do: :ping
  def parse_message(_), do: :ignore

  @impl true
  def ping_config do
    {%{"op" => "ping"}, 25_000}
  end

  @impl true
  def supported_feeds, do: [:ticker, :trades, :book, :liquidations, :funding_rate]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp channel_to_arg(%{feed: :ticker, pair: pair}) do
    %{"channel" => "tickers", "instId" => pair_to_inst(pair)}
  end

  defp channel_to_arg(%{feed: :trades, pair: pair}) do
    %{"channel" => "trades", "instId" => pair_to_inst(pair)}
  end

  defp channel_to_arg(%{feed: :book, pair: pair}) do
    %{"channel" => "books5", "instId" => pair_to_inst(pair)}
  end

  defp channel_to_arg(%{feed: :liquidations, pair: pair}) do
    %{"channel" => "liquidation-orders", "instType" => "SWAP", "instId" => pair_to_inst(pair)}
  end

  defp channel_to_arg(%{feed: :funding_rate, pair: pair}) do
    %{"channel" => "funding-rate", "instId" => pair_to_inst(pair)}
  end

  defp parse_tickers(data) do
    tickers =
      Enum.flat_map(data, fn item ->
        [
          %Ticker{
            source: :okx,
            pair: inst_to_pair(item["instId"]),
            price: parse_decimal(item["last"]),
            bid: parse_decimal(item["bidPx"]),
            ask: parse_decimal(item["askPx"]),
            volume_24h: parse_decimal(item["vol24h"]),
            change_24h: nil,
            high_24h: parse_decimal(item["high24h"]),
            low_24h: parse_decimal(item["low24h"]),
            timestamp: event_time(item["ts"])
          }
        ]
      end)

    {:ok, tickers}
  end

  defp parse_trades(data) do
    trades = Enum.flat_map(data, &build_trade/1)
    {:ok, trades}
  end

  defp build_trade(item) do
    with {:ok, price} <- safe_decimal(item["px"]),
         {:ok, qty} <- safe_decimal(item["sz"]) do
      side = if item["side"] == "buy", do: :buy, else: :sell

      [
        %Trade{
          source: :okx,
          pair: inst_to_pair(item["instId"]),
          trade_id: item["tradeId"],
          price: price,
          quantity: qty,
          side: side,
          timestamp: event_time(item["ts"])
        }
      ]
    else
      _ -> []
    end
  end

  defp parse_book_snapshot(data) do
    snapshots =
      Enum.flat_map(data, fn item ->
        bids = parse_levels(item["bids"])
        asks = parse_levels(item["asks"])

        [
          %BookSnapshot{
            source: :okx,
            pair: inst_to_pair(item["instId"]),
            bids: bids,
            asks: asks,
            sequence: parse_int(item["seqId"]),
            timestamp: event_time(item["ts"])
          }
        ]
      end)

    {:ok, snapshots}
  end

  defp parse_book_delta(data) do
    deltas =
      Enum.flat_map(data, fn item ->
        bids = parse_levels(item["bids"])
        asks = parse_levels(item["asks"])

        [
          %BookDelta{
            source: :okx,
            pair: inst_to_pair(item["instId"]),
            bids: bids,
            asks: asks,
            first_sequence: parse_int(item["prevSeqId"]),
            last_sequence: parse_int(item["seqId"]),
            timestamp: event_time(item["ts"])
          }
        ]
      end)

    {:ok, deltas}
  end

  defp parse_liquidations(data) do
    liqs = Enum.flat_map(data, &extract_liquidation_details/1)
    {:ok, liqs}
  end

  defp extract_liquidation_details(item) do
    details = Map.get(item, "details", [])
    pair = inst_to_pair(item["instId"])
    Enum.flat_map(details, &build_liquidation(&1, pair))
  end

  defp build_liquidation(detail, pair) do
    with {:ok, price} <- safe_decimal(detail["bkPx"]),
         {:ok, qty} <- safe_decimal(detail["sz"]) do
      side = if detail["side"] == "buy", do: :buy, else: :sell

      [
        %Liquidation{
          source: :okx,
          pair: pair,
          side: side,
          price: price,
          quantity: qty,
          timestamp: event_time(detail["ts"])
        }
      ]
    else
      _ -> []
    end
  end

  defp parse_funding_rates(data) do
    rates =
      Enum.flat_map(data, fn item ->
        [
          %FundingRate{
            source: :okx,
            pair: inst_to_pair(item["instId"]),
            rate: parse_decimal(item["fundingRate"]),
            next_funding_time: event_time(item["nextFundingTime"]),
            timestamp: event_time(item["ts"])
          }
        ]
      end)

    {:ok, rates}
  end

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

  defp pair_to_inst(:btc_usdt), do: "BTC-USDT-SWAP"
  defp pair_to_inst(:eth_usdt), do: "ETH-USDT-SWAP"
  defp pair_to_inst(:btc_usd), do: "BTC-USD-SWAP"

  defp pair_to_inst(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.replace("_", "-")
    |> Kernel.<>("-SWAP")
  end

  defp inst_to_pair(inst_id) when is_binary(inst_id) do
    inst_id
    |> String.replace("-SWAP", "")
    |> String.replace("-", "_")
    |> String.downcase()
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :unknown
  end

  defp inst_to_pair(_), do: :unknown

  defp parse_decimal(nil), do: nil

  defp parse_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp parse_decimal(value) when is_number(value), do: Decimal.new("#{value}")
  defp parse_decimal(_), do: nil

  defp parse_int(nil), do: nil

  defp parse_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp parse_int(value) when is_integer(value), do: value
  defp parse_int(_), do: nil

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
