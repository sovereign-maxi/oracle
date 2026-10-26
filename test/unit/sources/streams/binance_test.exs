defmodule Oracle.Sources.Streams.BinanceTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{Kline, Liquidation, Ticker, Trade}
  alias Oracle.Sources.Streams.Binance

  describe "name/0" do
    test "returns :binance" do
      assert Binance.name() == :binance
    end
  end

  describe "ws_url/1" do
    test "encodes channels in URL" do
      channels = [
        %{feed: :ticker, pair: :btc_usdt},
        %{feed: :trades, pair: :btc_usdt},
        %{feed: :klines, pair: :btc_usdt}
      ]

      url = Binance.ws_url(channels)
      assert String.contains?(url, "wss://stream.binance.com:9443/stream?streams=")
      assert String.contains?(url, "miniTicker")
      assert String.contains?(url, "trade")
      assert String.contains?(url, "kline_1m")
    end

    test "liquidation-only channels use the futures endpoint" do
      url = Binance.ws_url([%{feed: :liquidations, pair: :btc_usdt}])

      assert String.contains?(url, "wss://fstream.binance.com/stream?streams=")
      assert String.contains?(url, "forceOrder")
    end

    test "mixing liquidations with spot feeds raises" do
      channels = [
        %{feed: :liquidations, pair: :btc_usdt},
        %{feed: :ticker, pair: :btc_usdt}
      ]

      assert_raise ArgumentError, fn -> Binance.ws_url(channels) end
    end
  end

  describe "subscribe_messages/1" do
    test "returns empty list (subscriptions are in URL)" do
      assert Binance.subscribe_messages([]) == []
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

      assert {:ok, [%Ticker{} = ticker]} = Binance.parse_message(msg)
      assert ticker.source == :binance
      assert Decimal.equal?(ticker.price, Decimal.new("104523.45"))
    end

    test "maps wire symbols back to stack pair atoms" do
      msg = %{
        "stream" => "btcusdt@miniTicker",
        "data" => %{
          "E" => 1_704_067_200_000,
          "s" => "BTCUSDT",
          "c" => "104523.45",
          "h" => "105000.00",
          "l" => "102000.00",
          "v" => "28432.50"
        }
      }

      assert {:ok, [%Ticker{pair: :btc_usdt}]} = Binance.parse_message(msg)
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

      assert {:ok, [%Trade{} = trade]} = Binance.parse_message(msg)
      assert trade.source == :binance
      assert trade.side == :buy
      assert trade.trade_id == "123456"
    end

    test "parses liquidation message using average fill price" do
      msg = %{
        "stream" => "btcusdt@forceOrder",
        "data" => %{
          "o" => %{
            "s" => "BTCUSDT",
            "S" => "SELL",
            "p" => "0",
            "ap" => "103500.00",
            "q" => "0.25",
            "T" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, [%Liquidation{} = liq]} = Binance.parse_message(msg)
      assert liq.source == :binance
      assert liq.side == :sell
      assert Decimal.equal?(liq.price, Decimal.new("103500.00"))
    end

    test "rejects liquidation with missing or zero average price" do
      msg = %{
        "stream" => "btcusdt@forceOrder",
        "data" => %{
          "o" => %{
            "s" => "BTCUSDT",
            "S" => "SELL",
            "p" => "103500.00",
            "ap" => "0",
            "q" => "0.25",
            "T" => 1_704_067_200_000
          }
        }
      }

      assert {:error, :invalid_liquidation_data} = Binance.parse_message(msg)
    end

    test "ignores subscription confirmations" do
      assert :ignore = Binance.parse_message(%{"result" => nil})
    end

    test "handles ping messages" do
      assert :ping = Binance.parse_message(%{"ping" => 1})
    end

    test "ignores unknown messages" do
      assert :ignore = Binance.parse_message(%{"unknown" => "data"})
    end
  end

  describe "kline messages" do
    setup do
      %{message: kline_message()}
    end

    test "preserves exact exchange OHLC and bucket time", %{message: message} do
      assert {:ok, [%Kline{} = kline]} = Binance.parse_message(message)
      assert kline.source == :binance
      assert kline.pair == :btc_usdt
      assert kline.timeframe == :"1m"
      assert Decimal.equal?(kline.open, Decimal.new("100000.01"))
      assert Decimal.equal?(kline.high, Decimal.new("100010.02"))
      assert Decimal.equal?(kline.low, Decimal.new("99990.03"))
      assert Decimal.equal?(kline.close, Decimal.new("100005.04"))
      assert kline.timestamp == DateTime.from_unix!(1_704_067_200_000, :millisecond)
      assert kline.closed?
    end

    test "retains forming candles", %{message: message} do
      message = put_in(message, ["data", "k", "x"], false)
      assert {:ok, [%Kline{closed?: false}]} = Binance.parse_message(message)
    end

    test "rejects invalid prices, timestamps, and intervals", %{message: message} do
      for {field, value} <- [{"c", "0"}, {"t", 1}, {"i", "5m"}] do
        invalid = put_in(message, ["data", "k", field], value)
        assert {:error, :invalid_kline_data} = Binance.parse_message(invalid)
      end
    end
  end

  describe "ping_config/0" do
    test "returns nil (Binance uses WebSocket-level pings)" do
      assert Binance.ping_config() == nil
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Binance.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :klines in feeds
      assert :liquidations in feeds
      refute :book in feeds
    end
  end

  defp kline_message do
    %{
      "stream" => "btcusdt@kline_1m",
      "data" => %{
        "e" => "kline",
        "E" => 1_704_067_201_000,
        "s" => "BTCUSDT",
        "k" => %{
          "t" => 1_704_067_200_000,
          "i" => "1m",
          "s" => "BTCUSDT",
          "o" => "100000.01",
          "h" => "100010.02",
          "l" => "99990.03",
          "c" => "100005.04",
          "x" => true
        }
      }
    }
  end
end
