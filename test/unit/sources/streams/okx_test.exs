defmodule Oracle.Sources.Streams.OkxTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}
  alias Oracle.Sources.Streams.Okx

  describe "name/0" do
    test "returns :okx" do
      assert Okx.name() == :okx
    end
  end

  describe "ws_url/1" do
    test "returns single endpoint" do
      url = Okx.ws_url([])
      assert url == "wss://ws.okx.com:8443/ws/v5/public"
    end
  end

  describe "subscribe_messages/1" do
    test "returns subscribe message with args" do
      channels = [%{feed: :ticker, pair: :btc_usdt}]
      [msg] = Okx.subscribe_messages(channels)
      assert msg["op"] == "subscribe"
      assert is_list(msg["args"])
      [arg] = msg["args"]
      assert arg["channel"] == "tickers"
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = %{
        "arg" => %{"channel" => "tickers", "instId" => "BTC-USDT-SWAP"},
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "last" => "104523.45",
            "bidPx" => "104520.00",
            "askPx" => "104525.00",
            "vol24h" => "28432.50",
            "high24h" => "105000.00",
            "low24h" => "102000.00",
            "ts" => "1704067200000"
          }
        ]
      }

      assert {:ok, [%Ticker{} = ticker]} = Okx.parse_message(msg)
      assert ticker.source == :okx
    end

    test "parses trade message" do
      msg = %{
        "arg" => %{"channel" => "trades", "instId" => "BTC-USDT-SWAP"},
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "tradeId" => "12345",
            "px" => "104523.45",
            "sz" => "0.5",
            "side" => "buy",
            "ts" => "1704067200000"
          }
        ]
      }

      assert {:ok, [%Trade{} = trade]} = Okx.parse_message(msg)
      assert trade.source == :okx
      assert trade.side == :buy
    end

    test "parses book snapshot" do
      msg = %{
        "arg" => %{"channel" => "books5", "instId" => "BTC-USDT-SWAP"},
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "bids" => [["104520.00", "2.5", "0", "3"]],
            "asks" => [["104525.00", "1.8", "0", "2"]],
            "seqId" => "100",
            "ts" => "1704067200000"
          }
        ]
      }

      assert {:ok, [%BookSnapshot{} = snapshot]} = Okx.parse_message(msg)
      assert snapshot.source == :okx
    end

    test "parses book delta" do
      msg = %{
        "arg" => %{"channel" => "books5", "instId" => "BTC-USDT-SWAP"},
        "action" => "update",
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "bids" => [["104515.00", "1.0", "0", "1"]],
            "asks" => [],
            "prevSeqId" => "100",
            "seqId" => "101",
            "ts" => "1704067200000"
          }
        ]
      }

      assert {:ok, [%BookDelta{} = delta]} = Okx.parse_message(msg)
      assert delta.source == :okx
    end

    test "parses liquidation message" do
      msg = %{
        "arg" => %{"channel" => "liquidation-orders", "instId" => "BTC-USDT-SWAP"},
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "details" => [
              %{
                "side" => "sell",
                "bkPx" => "103500.00",
                "sz" => "0.25",
                "ts" => "1704067200000"
              }
            ]
          }
        ]
      }

      assert {:ok, [%Liquidation{} = liq]} = Okx.parse_message(msg)
      assert liq.source == :okx
      assert liq.side == :sell
    end

    test "parses funding rate message" do
      msg = %{
        "arg" => %{"channel" => "funding-rate", "instId" => "BTC-USDT-SWAP"},
        "data" => [
          %{
            "instId" => "BTC-USDT-SWAP",
            "fundingRate" => "0.0001",
            "nextFundingTime" => "1704070800000",
            "ts" => "1704067200000"
          }
        ]
      }

      assert {:ok, [%FundingRate{} = fr]} = Okx.parse_message(msg)
      assert fr.source == :okx
    end

    test "ignores subscribe events" do
      assert :ignore = Okx.parse_message(%{"event" => "subscribe"})
    end

    test "handles pong" do
      assert :ping = Okx.parse_message(%{"op" => "pong"})
    end
  end

  describe "ping_config/0" do
    test "returns ping config" do
      {msg, interval} = Okx.ping_config()
      assert msg["op"] == "ping"
      assert interval == 25_000
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Okx.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
      assert :liquidations in feeds
      assert :funding_rate in feeds
    end
  end
end
