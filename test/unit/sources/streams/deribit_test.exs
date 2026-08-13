defmodule Oracle.Sources.Streams.DeribitTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}
  alias Oracle.Sources.Streams.Deribit

  describe "name/0" do
    test "returns :deribit" do
      assert Deribit.name() == :deribit
    end
  end

  describe "ws_url/1" do
    test "returns single endpoint" do
      url = Deribit.ws_url([])
      assert url == "wss://www.deribit.com/ws/api/v2"
    end
  end

  describe "subscribe_messages/1" do
    test "returns JSON-RPC subscribe message" do
      channels = [%{feed: :ticker, pair: :btc_usd}]
      [msg] = Deribit.subscribe_messages(channels)
      assert msg["jsonrpc"] == "2.0"
      assert msg["method"] == "public/subscribe"
      assert is_list(msg["params"]["channels"])
    end
  end

  describe "unsubscribe_messages/1" do
    test "returns JSON-RPC unsubscribe message" do
      channels = [%{feed: :trades, pair: :btc_usd}]
      [msg] = Deribit.unsubscribe_messages(channels)
      assert msg["method"] == "public/unsubscribe"
    end
  end

  describe "parse_message/1" do
    test "parses ticker message" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "ticker.BTC-PERPETUAL.raw",
          "data" => %{
            "instrument_name" => "BTC-PERPETUAL",
            "last_price" => 104_523.45,
            "best_bid_price" => 104_520.00,
            "best_ask_price" => 104_525.00,
            "stats" => %{
              "volume" => 28_432.5,
              "price_change" => 2.35,
              "high" => 105_000.0,
              "low" => 102_000.0
            },
            "timestamp" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, result} = Deribit.parse_message(msg)
      ticker = Enum.find(result, &match?(%Ticker{}, &1))
      assert ticker.source == :deribit
    end

    test "drops tickers without a usable price" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "ticker.BTC-PERPETUAL.raw",
          "data" => %{
            "instrument_name" => "BTC-PERPETUAL",
            "timestamp" => 1_704_067_200_000
          }
        }
      }

      assert :ignore = Deribit.parse_message(msg)
    end

    test "keeps funding rate from price-less tickers" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "ticker.BTC-PERPETUAL.raw",
          "data" => %{
            "instrument_name" => "BTC-PERPETUAL",
            "funding_8h" => 0.0001,
            "timestamp" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, [%FundingRate{} = fr]} = Deribit.parse_message(msg)
      assert fr.source == :deribit
    end

    test "parses trade message, converting USD contract amount to base units" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "trades.BTC-PERPETUAL.raw",
          "data" => [
            %{
              "instrument_name" => "BTC-PERPETUAL",
              "trade_id" => "trade_1",
              "price" => 100_000.0,
              # Perpetual trade amounts are USD contracts on Deribit
              "amount" => 50_000.0,
              "direction" => "buy",
              "timestamp" => 1_704_067_200_000
            }
          ]
        }
      }

      assert {:ok, [%Trade{} = trade]} = Deribit.parse_message(msg)
      assert trade.source == :deribit
      assert trade.side == :buy
      assert Decimal.equal?(trade.quantity, Decimal.new("0.5"))
    end

    test "parses book snapshot with bare [price, amount] levels" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "book.BTC-PERPETUAL.none.20.100ms",
          "data" => %{
            "type" => "snapshot",
            "instrument_name" => "BTC-PERPETUAL",
            # Snapshot levels are bare [price, amount] pairs (no action tag)
            "bids" => [[104_520.00, 2.5]],
            "asks" => [[104_525.00, 1.8]],
            "change_id" => 100,
            "timestamp" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, [%BookSnapshot{} = snapshot]} = Deribit.parse_message(msg)
      assert snapshot.source == :deribit
      assert snapshot.sequence == 100
      assert [{bid_price, _}] = snapshot.bids
      assert Decimal.equal?(bid_price, Decimal.new("104520.0"))
      assert [{ask_price, _}] = snapshot.asks
      assert Decimal.equal?(ask_price, Decimal.new("104525.0"))
    end

    test "parses book delta" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "book.BTC-PERPETUAL.none.20.100ms",
          "data" => %{
            "instrument_name" => "BTC-PERPETUAL",
            "bids" => [["change", 104_520.00, 5.0]],
            "asks" => [["delete", 104_525.00, 0]],
            "prev_change_id" => 100,
            "change_id" => 101,
            "timestamp" => 1_704_067_200_000
          }
        }
      }

      assert {:ok, [%BookDelta{} = delta]} = Deribit.parse_message(msg)
      assert delta.source == :deribit
      assert delta.first_sequence == 100
      assert delta.last_sequence == 101
    end

    test "parses liquidation message, converting USD contract amount to base units" do
      msg = %{
        "method" => "subscription",
        "params" => %{
          "channel" => "liquidations.BTC-PERPETUAL.raw",
          "data" => [
            %{
              "instrument_name" => "BTC-PERPETUAL",
              "price" => 100_000.0,
              "amount" => 25_000.0,
              "direction" => "sell",
              "timestamp" => 1_704_067_200_000
            }
          ]
        }
      }

      assert {:ok, [%Liquidation{} = liq]} = Deribit.parse_message(msg)
      assert liq.source == :deribit
      assert liq.side == :sell
      assert Decimal.equal?(liq.quantity, Decimal.new("0.25"))
    end

    test "ignores RPC results" do
      assert :ignore = Deribit.parse_message(%{"id" => 1, "result" => ["channel1"]})
    end

    test "handles heartbeat test_request as ping" do
      msg = %{"method" => "heartbeat", "params" => %{"type" => "test_request"}}
      assert :ping = Deribit.parse_message(msg)
    end

    test "ignores regular heartbeat" do
      msg = %{"method" => "heartbeat", "params" => %{"type" => "heartbeat"}}
      assert :ignore = Deribit.parse_message(msg)
    end
  end

  describe "ping_config/0" do
    test "returns ping config" do
      {msg, interval} = Deribit.ping_config()
      assert msg["method"] == "public/test"
      assert interval == 15_000
    end
  end

  describe "supported_feeds/0" do
    test "returns expected feeds" do
      feeds = Deribit.supported_feeds()
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
      assert :liquidations in feeds
      assert :funding_rate in feeds
    end
  end
end
