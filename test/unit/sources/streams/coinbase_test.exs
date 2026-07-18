defmodule Oracle.Sources.Streams.CoinbaseTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, Ticker, Trade}
  alias Oracle.Sources.Streams.Coinbase

  describe "name/0" do
    test "returns :coinbase" do
      assert Coinbase.name() == :coinbase
    end
  end

  describe "ws_url/1" do
    test "returns single endpoint" do
      url = Coinbase.ws_url([%{feed: :ticker, pair: :btc_usd}])
      assert url == "wss://ws-feed.exchange.coinbase.com"
    end
  end

  describe "subscribe_messages/1" do
    test "returns subscribe message with product_ids" do
      channels = [%{feed: :ticker, pair: :btc_usd}]
      [msg] = Coinbase.subscribe_messages(channels)
      assert msg["type"] == "subscribe"
      assert "BTC-USD" in msg["product_ids"]
      assert "ticker" in msg["channels"]
    end
  end

  describe "unsubscribe_messages/1" do
    test "returns unsubscribe message" do
      channels = [%{feed: :trades, pair: :eth_usd}]
      [msg] = Coinbase.unsubscribe_messages(channels)
      assert msg["type"] == "unsubscribe"
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = %{
        "type" => "ticker",
        "product_id" => "BTC-USD",
        "price" => "104523.45",
        "best_bid" => "104520.00",
        "best_ask" => "104525.00",
        "volume_24h" => "28432.50",
        "low_24h" => "102000.00",
        "high_24h" => "105000.00",
        "time" => "2024-01-01T12:00:00.000000Z"
      }

      assert {:ok, [%Ticker{} = ticker]} = Coinbase.parse_message(msg)
      assert ticker.source == :coinbase
      assert ticker.pair == :btc_usd
    end

    test "parses match (trade) message" do
      msg = %{
        "type" => "match",
        "product_id" => "BTC-USD",
        "trade_id" => 12_345,
        "price" => "104523.45",
        "size" => "0.5",
        "side" => "buy",
        "time" => "2024-01-01T12:00:00.000000Z"
      }

      assert {:ok, [%Trade{} = trade]} = Coinbase.parse_message(msg)
      assert trade.source == :coinbase
      assert trade.side == :buy
    end

    test "parses snapshot message" do
      msg = %{
        "type" => "snapshot",
        "product_id" => "BTC-USD",
        "bids" => [["104520.00", "2.5"], ["104510.00", "3.0"]],
        "asks" => [["104525.00", "1.8"]]
      }

      assert {:ok, [%BookSnapshot{} = snapshot]} = Coinbase.parse_message(msg)
      assert snapshot.source == :coinbase
      assert length(snapshot.bids) == 2
      assert length(snapshot.asks) == 1
    end

    test "parses l2update message" do
      msg = %{
        "type" => "l2update",
        "product_id" => "BTC-USD",
        "changes" => [
          ["buy", "104520.00", "5.0"],
          ["sell", "104525.00", "0"]
        ],
        "time" => "2024-01-01T12:00:00.000000Z"
      }

      assert {:ok, [%BookDelta{} = delta]} = Coinbase.parse_message(msg)
      assert delta.source == :coinbase
      assert length(delta.bids) == 1
      assert length(delta.asks) == 1
    end

    test "ignores heartbeat" do
      assert :ignore = Coinbase.parse_message(%{"type" => "heartbeat"})
    end

    test "ignores subscriptions" do
      assert :ignore = Coinbase.parse_message(%{"type" => "subscriptions"})
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Coinbase.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
    end
  end
end
