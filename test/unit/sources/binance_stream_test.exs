defmodule Oracle.Sources.BinanceStreamTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, Liquidation, Ticker, Trade}
  alias Oracle.Sources.BinanceStream

  describe "name/0" do
    test "returns :binance" do
      assert BinanceStream.name() == :binance
    end
  end

  describe "ws_url/1" do
    test "encodes channels in URL" do
      channels = [
        %{feed: :ticker, pair: :btcusdt},
        %{feed: :trades, pair: :btcusdt}
      ]

      url = BinanceStream.ws_url(channels)
      assert String.contains?(url, "wss://stream.binance.com:9443/stream?streams=")
      assert String.contains?(url, "miniTicker")
      assert String.contains?(url, "trade")
    end
  end

  describe "subscribe_messages/1" do
    test "returns empty list (subscriptions are in URL)" do
      assert BinanceStream.subscribe_messages([]) == []
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = %{
        "stream" => "btcusdt@miniTicker",
        "data" => %{
          "e" => "24hrMiniTicker",
          "E" => 1_704_067_200_000,
          "s" => "BTCUSDT",
          "c" => "104523.45",
          "h" => "105000.00",
          "l" => "102000.00",
          "v" => "28432.50"
        }
      }

      assert {:ok, [%Ticker{} = ticker]} = BinanceStream.parse_message(msg)
      assert ticker.source == :binance
      assert Decimal.equal?(ticker.price, Decimal.new("104523.45"))
    end

    test "parses trade message" do
      msg = %{
        "stream" => "btcusdt@trade",
        "data" => %{
          "e" => "trade",
          "E" => 1_704_067_200_000,
          "T" => 1_704_067_200_000,
          "s" => "BTCUSDT",
          "t" => 123_456,
          "p" => "104523.45",
          "q" => "0.5",
          "m" => false
        }
      }

      assert {:ok, [%Trade{} = trade]} = BinanceStream.parse_message(msg)
      assert trade.source == :binance
      assert trade.side == :buy
      assert trade.trade_id == "123456"
    end

    test "parses depth message" do
      msg = %{
        "stream" => "btcusdt@depth",
        "data" => %{
          "e" => "depthUpdate",
          "E" => 1_704_067_200_000,
          "s" => "BTCUSDT",
          "U" => 100,
          "u" => 105,
          "b" => [["104520.00", "2.5"]],
          "a" => [["104525.00", "1.8"]]
        }
      }

      assert {:ok, [%BookDelta{} = delta]} = BinanceStream.parse_message(msg)
      assert delta.source == :binance
      assert delta.first_sequence == 100
      assert delta.last_sequence == 105
      assert length(delta.bids) == 1
      assert length(delta.asks) == 1
    end

    test "parses liquidation message" do
      msg = %{
        "stream" => "btcusdt@forceOrder",
        "data" => %{
          "o" => %{
            "s" => "BTCUSDT",
            "S" => "SELL",
            "p" => "103500.00",
            "q" => "0.25",
            "T" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, [%Liquidation{} = liq]} = BinanceStream.parse_message(msg)
      assert liq.source == :binance
      assert liq.side == :sell
    end

    test "ignores subscription confirmations" do
      assert :ignore = BinanceStream.parse_message(%{"result" => nil})
    end

    test "handles ping messages" do
      assert :ping = BinanceStream.parse_message(%{"ping" => 1})
    end

    test "ignores unknown messages" do
      assert :ignore = BinanceStream.parse_message(%{"unknown" => "data"})
    end
  end

  describe "ping_config/0" do
    test "returns nil (Binance uses WebSocket-level pings)" do
      assert BinanceStream.ping_config() == nil
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = BinanceStream.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
      assert :liquidations in feeds
    end
  end
end
