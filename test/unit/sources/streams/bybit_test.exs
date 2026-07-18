defmodule Oracle.Sources.Streams.BybitTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, Liquidation, Ticker, Trade}
  alias Oracle.Sources.Streams.Bybit

  describe "name/0" do
    test "returns :bybit" do
      assert Bybit.name() == :bybit
    end
  end

  describe "ws_url/1" do
    test "returns single endpoint" do
      url = Bybit.ws_url([])
      assert url == "wss://stream.bybit.com/v5/public/linear"
    end
  end

  describe "subscribe_messages/1" do
    test "returns subscribe message with topics" do
      channels = [%{feed: :ticker, pair: :btcusdt}, %{feed: :trades, pair: :btcusdt}]
      [msg] = Bybit.subscribe_messages(channels)
      assert msg["op"] == "subscribe"
      assert is_list(msg["args"])
      assert length(msg["args"]) == 2
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = %{
        "topic" => "tickers.BTCUSDT",
        "type" => "snapshot",
        "data" => %{
          "symbol" => "BTCUSDT",
          "lastPrice" => "104523.45",
          "bid1Price" => "104520.00",
          "ask1Price" => "104525.00",
          "volume24h" => "28432.50",
          "price24hPcnt" => "0.0235",
          "highPrice24h" => "105000.00",
          "lowPrice24h" => "102000.00",
          "ts" => 1_704_067_200_000
        }
      }

      assert {:ok, result} = Bybit.parse_message(msg)
      ticker = Enum.find(result, &match?(%Ticker{}, &1))
      assert ticker.source == :bybit
      assert Decimal.equal?(ticker.price, Decimal.new("104523.45"))
    end

    test "parses trade message" do
      msg = %{
        "topic" => "publicTrade.BTCUSDT",
        "type" => "snapshot",
        "data" => [
          %{
            "s" => "BTCUSDT",
            "i" => "trade_1",
            "p" => "104523.45",
            "v" => "0.5",
            "S" => "Buy",
            "T" => 1_704_067_200_000
          }
        ]
      }

      assert {:ok, [%Trade{} = trade]} = Bybit.parse_message(msg)
      assert trade.source == :bybit
      assert trade.side == :buy
    end

    test "parses book snapshot" do
      msg = %{
        "topic" => "orderbook.50.BTCUSDT",
        "type" => "snapshot",
        "data" => %{
          "b" => [["104520.00", "2.5"]],
          "a" => [["104525.00", "1.8"]],
          "seq" => 100,
          "ts" => 1_704_067_200_000
        }
      }

      assert {:ok, [%BookSnapshot{} = snapshot]} = Bybit.parse_message(msg)
      assert snapshot.source == :bybit
      assert snapshot.sequence == 100
    end

    test "parses book delta" do
      msg = %{
        "topic" => "orderbook.50.BTCUSDT",
        "type" => "delta",
        "data" => %{
          "b" => [["104515.00", "1.0"]],
          "a" => [["104530.00", "0"]],
          "seq" => 101,
          "ts" => 1_704_067_200_000
        }
      }

      assert {:ok, [%BookDelta{} = delta]} = Bybit.parse_message(msg)
      assert delta.source == :bybit
    end

    test "parses liquidation" do
      msg = %{
        "topic" => "liquidation.BTCUSDT",
        "type" => "snapshot",
        "data" => %{
          "symbol" => "BTCUSDT",
          "side" => "Sell",
          "price" => "103500.00",
          "size" => "0.25",
          "updatedTime" => 1_704_067_200_000
        }
      }

      assert {:ok, [%Liquidation{} = liq]} = Bybit.parse_message(msg)
      assert liq.source == :bybit
      assert liq.side == :sell
    end

    test "handles pong" do
      assert :ping = Bybit.parse_message(%{"op" => "pong"})
    end

    test "ignores subscribe confirmations" do
      assert :ignore = Bybit.parse_message(%{"success" => true})
    end
  end

  describe "ping_config/0" do
    test "returns ping config" do
      {msg, interval} = Bybit.ping_config()
      assert msg["op"] == "ping"
      assert interval == 20_000
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Bybit.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
      assert :liquidations in feeds
      assert :funding_rate in feeds
    end
  end
end
