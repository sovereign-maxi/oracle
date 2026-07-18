defmodule Oracle.Sources.Streams.AlpacaTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{Ticker, Trade}
  alias Oracle.Sources.Streams.Alpaca

  # `:mstr_usd` is not a registered atom in the oracle library on its
  # own — this ensures it exists so `symbol_to_pair/1`'s
  # `String.to_existing_atom/1` succeeds.
  _ = :mstr_usd

  describe "name/0" do
    test "returns :alpaca" do
      assert Alpaca.name() == :alpaca
    end
  end

  describe "ws_url/1" do
    test "returns the SIP consolidated feed endpoint by default" do
      assert Alpaca.ws_url([%{feed: :ticker, pair: :mstr_usd}]) ==
               "wss://stream.data.alpaca.markets/v2/sip"
    end

    test "exposes IEX-only URL for the free tier" do
      assert Alpaca.ws_url_iex() == "wss://stream.data.alpaca.markets/v2/iex"
    end
  end

  describe "auth_message/2" do
    test "builds an auth frame with the supplied credentials" do
      assert Alpaca.auth_message("KEY_ID", "SECRET") == %{
               "action" => "auth",
               "key" => "KEY_ID",
               "secret" => "SECRET"
             }
    end
  end

  describe "subscribe_messages/1" do
    test "groups channels by feed and maps pairs to Alpaca symbols" do
      channels = [
        %{feed: :ticker, pair: :mstr_usd},
        %{feed: :trades, pair: :mstr_usd}
      ]

      [msg] = Alpaca.subscribe_messages(channels)

      assert msg["action"] == "subscribe"
      assert msg["quotes"] == ["MSTR"]
      assert msg["trades"] == ["MSTR"]
    end
  end

  describe "unsubscribe_messages/1" do
    test "emits an unsubscribe frame with the same channel layout" do
      channels = [%{feed: :ticker, pair: :mstr_usd}]
      [msg] = Alpaca.unsubscribe_messages(channels)

      assert msg["action"] == "unsubscribe"
      assert msg["quotes"] == ["MSTR"]
    end
  end

  describe "parse_message/1" do
    test "parses a quote message and computes midpoint" do
      msg = %{
        "T" => "q",
        "S" => "MSTR",
        "bp" => "400.10",
        "ap" => "400.30",
        "bs" => 1,
        "as" => 2,
        "t" => "2024-01-01T15:30:00.123456789Z"
      }

      assert {:ok, [%Ticker{} = t]} = Alpaca.parse_message(msg)
      assert t.source == :alpaca
      assert t.pair == :mstr_usd
      assert Decimal.equal?(t.bid, Decimal.new("400.10"))
      assert Decimal.equal?(t.ask, Decimal.new("400.30"))
      assert Decimal.equal?(t.price, Decimal.new("400.20"))
      assert t.volume_24h == nil
    end

    test "parses a trade message" do
      msg = %{
        "T" => "t",
        "S" => "MSTR",
        "p" => "400.15",
        "s" => 10,
        "i" => 12_345,
        "t" => "2024-01-01T15:30:00.123456789Z"
      }

      assert {:ok, [%Trade{} = tr]} = Alpaca.parse_message(msg)
      assert tr.source == :alpaca
      assert tr.pair == :mstr_usd
      assert tr.trade_id == "12345"
      assert Decimal.equal?(tr.price, Decimal.new("400.15"))
      assert tr.side == nil
    end

    test "ignores auth success + subscription confirmation frames" do
      assert Alpaca.parse_message(%{"T" => "success", "msg" => "authenticated"}) ==
               :ignore

      assert Alpaca.parse_message(%{"T" => "subscription", "quotes" => ["MSTR"]}) ==
               :ignore
    end

    test "surfaces alpaca error frames as {:error, {:alpaca_error, msg}}" do
      err = %{"T" => "error", "code" => 402, "msg" => "auth failed"}
      assert {:error, {:alpaca_error, ^err}} = Alpaca.parse_message(err)
    end

    test "ignores unknown frame types" do
      assert Alpaca.parse_message(%{"T" => "n"}) == :ignore
    end

    test "returns error on missing quote fields" do
      msg = %{"T" => "q", "S" => "MSTR", "bp" => nil, "ap" => "400.00"}
      assert {:error, :invalid_quote_data} = Alpaca.parse_message(msg)
    end

    test "maps unregistered symbols to :unknown" do
      msg = %{
        "T" => "q",
        "S" => "NEVER_REGISTERED_TICKER",
        "bp" => "1.00",
        "ap" => "1.10",
        "t" => "2024-01-01T15:30:00Z"
      }

      assert {:ok, [%Ticker{pair: :unknown}]} = Alpaca.parse_message(msg)
    end
  end

  describe "supported_feeds/0" do
    test "advertises quote + trade feeds" do
      assert Alpaca.supported_feeds() == [:ticker, :trades]
    end
  end

  describe "ping_config/0" do
    test "returns nil (Alpaca does not require app-level pings)" do
      assert Alpaca.ping_config() == nil
    end
  end
end
