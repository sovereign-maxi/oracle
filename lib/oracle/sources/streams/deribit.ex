defmodule Oracle.Sources.Streams.Deribit do
  @moduledoc """
  Deribit WebSocket streaming adapter.

  Uses JSON-RPC 2.0 protocol for subscribe/unsubscribe.

  ## WebSocket Endpoint

      wss://www.deribit.com/ws/api/v2

  ## Supported Feeds

  - `:ticker` - Real-time ticker data
  - `:trades` - Trade executions
  - `:book` - Order book snapshots and deltas
  - `:liquidations` - Forced liquidations
  - `:funding_rate` - Perpetual futures funding rates
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}

  @ws_url "wss://www.deribit.com/ws/api/v2"

  @impl true
  def name, do: :deribit

  @impl true
  def ws_url(_channels), do: @ws_url

  @impl true
  def subscribe_messages(channels) do
    channel_names = Enum.map(channels, &channel_to_deribit/1)

    [
      %{
        "jsonrpc" => "2.0",
        "id" => 1,
        "method" => "public/subscribe",
        "params" => %{"channels" => channel_names}
      }
    ]
  end

  @impl true
  def unsubscribe_messages(channels) do
    channel_names = Enum.map(channels, &channel_to_deribit/1)

    [
      %{
        "jsonrpc" => "2.0",
        "id" => 2,
        "method" => "public/unsubscribe",
        "params" => %{"channels" => channel_names}
      }
    ]
  end

  @impl true
  def parse_message(%{
        "method" => "subscription",
        "params" => %{"channel" => channel, "data" => data}
      }) do
    cond do
      String.starts_with?(channel, "ticker.") ->
        parse_ticker(data)

      String.starts_with?(channel, "trades.") ->
        parse_trades(data)

      String.starts_with?(channel, "book.") and match?(%{"type" => "snapshot"}, data) ->
        parse_book_snapshot(data)

      String.starts_with?(channel, "book.") ->
        parse_book_delta(data)

      String.starts_with?(channel, "liquidations.") ->
        parse_liquidations(data)

      true ->
        :ignore
    end
  end

  def parse_message(%{"id" => _, "result" => _}), do: :ignore
  def parse_message(%{"method" => "heartbeat", "params" => %{"type" => "test_request"}}), do: :ping
  def parse_message(%{"method" => "heartbeat"}), do: :ignore
  def parse_message(_), do: :ignore

  @impl true
  def ping_config do
    {%{"jsonrpc" => "2.0", "id" => 0, "method" => "public/test"}, 15_000}
  end

  @impl true
  def supported_feeds, do: [:ticker, :trades, :book, :liquidations, :funding_rate]

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  defp channel_to_deribit(%{feed: :ticker, pair: pair}) do
    "ticker.#{pair_to_instrument(pair)}.raw"
  end

  defp channel_to_deribit(%{feed: :trades, pair: pair}) do
    "trades.#{pair_to_instrument(pair)}.raw"
  end

  defp channel_to_deribit(%{feed: :book, pair: pair}) do
    "book.#{pair_to_instrument(pair)}.none.20.100ms"
  end

  defp channel_to_deribit(%{feed: :liquidations, pair: pair}) do
    "liquidations.#{pair_to_instrument(pair)}.raw"
  end

  defp channel_to_deribit(%{feed: :funding_rate, pair: pair}) do
    "ticker.#{pair_to_instrument(pair)}.raw"
  end

  defp parse_ticker(data) do
    if is_map(data) do
      case ticker_structs(data) do
        [] -> :ignore
        structs -> {:ok, structs}
      end
    else
      :ignore
    end
  end

  defp ticker_structs(data) do
    pair = instrument_to_pair(data["instrument_name"])

    [ticker_struct(data, pair), funding_struct(data, pair)]
    |> Enum.reject(&is_nil/1)
  end

  # A ticker is only emitted with a usable, positive price — never partial.
  defp ticker_struct(data, pair) do
    case positive_decimal(data["last_price"]) do
      {:ok, price} ->
        %Ticker{
          source: :deribit,
          pair: pair,
          price: price,
          bid: parse_decimal(data["best_bid_price"]),
          ask: parse_decimal(data["best_ask_price"]),
          volume_24h: parse_decimal(data["stats"] && data["stats"]["volume"]),
          change_24h: parse_decimal(data["stats"] && data["stats"]["price_change"]),
          high_24h: parse_decimal(data["stats"] && data["stats"]["high"]),
          low_24h: parse_decimal(data["stats"] && data["stats"]["low"]),
          timestamp: event_time(data["timestamp"])
        }

      {:error, _} ->
        nil
    end
  end

  defp funding_struct(data, pair) do
    case parse_decimal(data["funding_8h"]) do
      %Decimal{} = rate ->
        %FundingRate{
          source: :deribit,
          pair: pair,
          rate: rate,
          next_funding_time: nil,
          timestamp: event_time(data["timestamp"])
        }

      _ ->
        nil
    end
  end

  defp parse_trades(data) when is_list(data) do
    trades = Enum.flat_map(data, &build_trade/1)
    {:ok, trades}
  end

  defp parse_trades(_), do: :ignore

  defp build_trade(item) do
    with {:ok, price} <- positive_decimal(item["price"]),
         {:ok, qty} <- contract_quantity(item["amount"], price) do
      side = if item["direction"] == "buy", do: :buy, else: :sell

      [
        %Trade{
          source: :deribit,
          pair: instrument_to_pair(item["instrument_name"]),
          trade_id: item["trade_id"],
          price: price,
          quantity: qty,
          side: side,
          timestamp: event_time(item["timestamp"])
        }
      ]
    else
      _ -> []
    end
  end

  defp parse_book_snapshot(data) do
    bids = parse_levels(data["bids"])
    asks = parse_levels(data["asks"])

    {:ok,
     [
       %BookSnapshot{
         source: :deribit,
         pair: instrument_to_pair(data["instrument_name"]),
         bids: bids,
         asks: asks,
         sequence: data["change_id"],
         timestamp: event_time(data["timestamp"])
       }
     ]}
  end

  defp parse_book_delta(data) do
    bids = parse_levels(data["bids"])
    asks = parse_levels(data["asks"])

    {:ok,
     [
       %BookDelta{
         source: :deribit,
         pair: instrument_to_pair(data["instrument_name"]),
         bids: bids,
         asks: asks,
         first_sequence: data["prev_change_id"],
         last_sequence: data["change_id"],
         timestamp: event_time(data["timestamp"])
       }
     ]}
  end

  defp parse_liquidations(data) when is_list(data) do
    liqs = Enum.flat_map(data, &build_liquidation/1)
    {:ok, liqs}
  end

  defp parse_liquidations(_), do: :ignore

  defp build_liquidation(item) do
    with {:ok, price} <- positive_decimal(item["price"]),
         {:ok, qty} <- contract_quantity(item["amount"], price) do
      side = if item["direction"] == "buy", do: :buy, else: :sell

      [
        %Liquidation{
          source: :deribit,
          pair: instrument_to_pair(item["instrument_name"]),
          side: side,
          price: price,
          quantity: qty,
          timestamp: event_time(item["timestamp"])
        }
      ]
    else
      _ -> []
    end
  end

  # Deribit book levels: ["new"|"change"|"delete", price, amount] on delta
  # updates; bare [price, amount] on snapshots.
  defp parse_levels(levels) when is_list(levels) do
    Enum.flat_map(levels, fn
      [action, price_num, qty_num] when action in ["new", "change"] ->
        with {:ok, price} <- safe_decimal(price_num),
             {:ok, qty} <- safe_decimal(qty_num) do
          [{price, qty}]
        else
          _ -> []
        end

      ["delete", price_num, _qty_num] ->
        case safe_decimal(price_num) do
          {:ok, price} -> [{price, Decimal.new(0)}]
          _ -> []
        end

      [price_num, qty_num] ->
        with {:ok, price} <- safe_decimal(price_num),
             {:ok, qty} <- safe_decimal(qty_num) do
          [{price, qty}]
        else
          _ -> []
        end

      _ ->
        []
    end)
  end

  defp parse_levels(_), do: []

  defp pair_to_instrument(:btc_usd), do: "BTC-PERPETUAL"
  defp pair_to_instrument(:eth_usd), do: "ETH-PERPETUAL"
  defp pair_to_instrument(:btc_usdt), do: "BTC-PERPETUAL"

  defp pair_to_instrument(pair) do
    pair
    |> Atom.to_string()
    |> String.upcase()
    |> String.split("_")
    |> List.first()
    |> Kernel.<>("-PERPETUAL")
  end

  defp instrument_to_pair(name) when is_binary(name) do
    name
    |> String.replace("-PERPETUAL", "_usd")
    |> String.downcase()
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :unknown
  end

  defp instrument_to_pair(_), do: :unknown

  defp parse_decimal(nil), do: nil

  defp parse_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> nil
    end
  end

  defp parse_decimal(value) when is_number(value), do: Decimal.new("#{value}")
  defp parse_decimal(_), do: nil

  defp event_time(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, dt} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp event_time(_), do: DateTime.utc_now()

  defp safe_decimal(nil), do: {:error, :nil_value}

  defp safe_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> {:error, {:invalid_decimal, value}}
    end
  end

  defp safe_decimal(value) when is_number(value), do: {:ok, Decimal.new("#{value}")}
  defp safe_decimal(_), do: {:error, :invalid_value}

  defp positive_decimal(value) do
    case parse_decimal(value) do
      %Decimal{} = decimal ->
        if Decimal.positive?(decimal), do: {:ok, decimal}, else: {:error, :non_positive}

      _ ->
        {:error, :invalid_decimal}
    end
  end

  # Deribit reports trade and liquidation amounts in USD contracts on the
  # perpetual instruments this adapter subscribes. Convert to base units
  # so quantity semantics match every other adapter.
  defp contract_quantity(usd_amount, price) do
    case safe_decimal(usd_amount) do
      {:ok, usd} ->
        if Decimal.gt?(usd, Decimal.new(0)) do
          {:ok, Decimal.div(usd, price)}
        else
          {:error, :invalid_amount}
        end

      error ->
        error
    end
  end
end
