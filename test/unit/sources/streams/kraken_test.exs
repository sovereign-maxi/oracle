defmodule Oracle.Sources.Streams.KrakenTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, Ticker, Trade}
  alias Oracle.Sources.Streams.Kraken

  describe "name/0" do
    test "returns :kraken" do
      assert Kraken.name() == :kraken
    end
  end

  describe "ws_url/1" do
    test "returns single endpoint" do
      url = Kraken.ws_url([])
      assert url == "wss://ws.kraken.com"
    end
  end

  describe "subscribe_messages/1" do
    test "returns subscribe message with pairs" do
      channels = [%{feed: :ticker, pair: :btc_usd}]
      [msg] = Kraken.subscribe_messages(channels)
      assert msg["event"] == "subscribe"
      assert "XBT/USD" in msg["pair"]
      assert msg["subscription"]["name"] == "ticker"
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = [
        0,
        %{
          "c" => ["104523.45", "1.0"],
          "b" => ["104520.00", "2.5"],
          "a" => ["104525.00", "1.8"],
          "v" => ["1000", "28432.50"],
          "h" => ["105000.00", "105000.00"],
          "l" => ["102000.00", "102000.00"]
        },
        "ticker",
        "XBT/USD"
      ]

      assert {:ok, [%Ticker{} = ticker]} = Kraken.parse_message(msg)
      assert ticker.source == :kraken
      assert ticker.pair == :btc_usd
    end

    test "parses trade message" do
      msg = [
        0,
        [["104523.45", "0.5", "1704067200.0", "b", "l", ""]],
        "trade",
        "XBT/USD"
      ]

      assert {:ok, [%Trade{} = trade]} = Kraken.parse_message(msg)
      assert trade.source == :kraken
      assert trade.side == :buy
    end

    test "parses book snapshot" do
      msg = [
        0,
        %{
          "as" => [["104525.00", "1.8", "1704067200.0"]],
          "bs" => [["104520.00", "2.5", "1704067200.0"]]
        },
        "book-10",
        "XBT/USD"
      ]

      assert {:ok, [%BookSnapshot{} = snapshot]} = Kraken.parse_message(msg)
      assert snapshot.source == :kraken
    end

    test "parses book delta" do
      msg = [
        0,
        %{
          "a" => [["104525.00", "3.0", "1704067201.0"]],
          "b" => [["104520.00", "0.0", "1704067201.0"]]
        },
        "book-10",
        "XBT/USD"
      ]

      assert {:ok, [%BookDelta{} = delta]} = Kraken.parse_message(msg)
      assert delta.source == :kraken
    end

    test "parses two-sided book updates (5-element array)" do
      msg = [
        0,
        %{"a" => [["104525.00", "3.0", "1704067201.0"]]},
        %{"b" => [["104520.00", "1.5", "1704067201.0"]]},
        "book-10",
        "XBT/USD"
      ]

      assert {:ok, [%BookDelta{} = delta]} = Kraken.parse_message(msg)
      assert delta.source == :kraken
      assert length(delta.asks) == 1
      assert length(delta.bids) == 1
    end

    test "surfaces subscription errors" do
      msg = %{
        "event" => "subscriptionStatus",
        "status" => "error",
        "errorMessage" => "Unknown pair",
        "pair" => "XBT/USD"
      }

      assert {:error, {:subscribe_error, ^msg}} = Kraken.parse_message(msg)
    end

    test "ignores heartbeat" do
      assert :ignore = Kraken.parse_message(%{"event" => "heartbeat"})
    end

    test "handles pong" do
      assert :ping = Kraken.parse_message(%{"event" => "pong"})
    end

    test "ignores subscription status" do
      assert :ignore = Kraken.parse_message(%{"event" => "subscriptionStatus"})
    end
  end

  describe "ping_config/0" do
    test "returns ping config" do
      {msg, interval} = Kraken.ping_config()
      assert msg["event"] == "ping"
      assert interval == 30_000
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Kraken.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
    end
  end
end
