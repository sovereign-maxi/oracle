defmodule Oracle.FeedsTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{BookDelta, BookSnapshot, FundingRate, Liquidation, Ticker, Trade}

  describe "Ticker" do
    test "has expected struct fields" do
      now = DateTime.utc_now()

      ticker = %Ticker{
        source: :binance,
        pair: :btc_usdt,
        price: Decimal.new("104523.45"),
        bid: Decimal.new("104520.00"),
        ask: Decimal.new("104525.00"),
        volume_24h: Decimal.new("28432.5"),
        change_24h: Decimal.new("2.35"),
        high_24h: Decimal.new("105000.00"),
        low_24h: Decimal.new("102000.00"),
        timestamp: now
      }

      assert ticker.source == :binance
      assert ticker.pair == :btc_usdt
      assert Decimal.equal?(ticker.price, Decimal.new("104523.45"))
      assert Decimal.equal?(ticker.bid, Decimal.new("104520.00"))
      assert Decimal.equal?(ticker.ask, Decimal.new("104525.00"))
      assert ticker.timestamp == now
    end

    test "defaults version to 1" do
      assert %Ticker{}.version == 1
    end

    test "nullable fields default to nil" do
      ticker = %Ticker{source: :binance, pair: :btc_usdt}
      assert is_nil(ticker.price)
      assert is_nil(ticker.bid)
      assert is_nil(ticker.ask)
    end
  end

  describe "Trade" do
    test "has expected struct fields" do
      trade = %Trade{
        source: :coinbase,
        pair: :eth_usd,
        trade_id: "12345678",
        price: Decimal.new("3250.00"),
        quantity: Decimal.new("1.5"),
        side: :buy,
        timestamp: DateTime.utc_now()
      }

      assert trade.source == :coinbase
      assert trade.trade_id == "12345678"
      assert trade.side == :buy
    end

    test "defaults version to 1" do
      assert %Trade{}.version == 1
    end

    test "side can be :buy or :sell" do
      assert %Trade{side: :buy}.side == :buy
      assert %Trade{side: :sell}.side == :sell
    end
  end

  describe "BookSnapshot" do
    test "has expected struct fields" do
      bids = [
        {Decimal.new("104520.00"), Decimal.new("2.5")},
        {Decimal.new("104510.00"), Decimal.new("3.0")}
      ]

      asks = [
        {Decimal.new("104525.00"), Decimal.new("1.8")},
        {Decimal.new("104530.00"), Decimal.new("4.2")}
      ]

      snapshot = %BookSnapshot{
        source: :kraken,
        pair: :btc_usd,
        bids: bids,
        asks: asks,
        sequence: 42_000_001,
        timestamp: DateTime.utc_now()
      }

      assert snapshot.source == :kraken
      assert length(snapshot.bids) == 2
      assert length(snapshot.asks) == 2
      assert snapshot.sequence == 42_000_001
    end

    test "defaults version to 1" do
      assert %BookSnapshot{}.version == 1
    end

    test "defaults bids and asks to empty lists" do
      snapshot = %BookSnapshot{source: :binance, pair: :btc_usdt}
      assert snapshot.bids == []
      assert snapshot.asks == []
    end
  end

  describe "BookDelta" do
    test "has expected struct fields" do
      delta = %BookDelta{
        source: :bybit,
        pair: :btc_usdt,
        bids: [{Decimal.new("104515.00"), Decimal.new("1.0")}],
        asks: [{Decimal.new("104530.00"), Decimal.new("0")}],
        first_sequence: 100,
        last_sequence: 105,
        timestamp: DateTime.utc_now()
      }

      assert delta.source == :bybit
      assert delta.first_sequence == 100
      assert delta.last_sequence == 105
    end

    test "defaults version to 1" do
      assert %BookDelta{}.version == 1
    end

    test "defaults bids and asks to empty lists" do
      delta = %BookDelta{source: :binance, pair: :btc_usdt}
      assert delta.bids == []
      assert delta.asks == []
    end
  end

  describe "Liquidation" do
    test "has expected struct fields" do
      liq = %Liquidation{
        source: :bybit,
        pair: :btc_usdt,
        side: :sell,
        price: Decimal.new("103500.00"),
        quantity: Decimal.new("0.25"),
        timestamp: DateTime.utc_now()
      }

      assert liq.source == :bybit
      assert liq.side == :sell
      assert Decimal.equal?(liq.price, Decimal.new("103500.00"))
    end

    test "defaults version to 1" do
      assert %Liquidation{}.version == 1
    end
  end

  describe "FundingRate" do
    test "has expected struct fields" do
      now = DateTime.utc_now()
      next_funding = DateTime.add(now, 3600, :second)

      fr = %FundingRate{
        source: :okx,
        pair: :btc_usdt,
        rate: Decimal.new("0.0001"),
        next_funding_time: next_funding,
        timestamp: now
      }

      assert fr.source == :okx
      assert Decimal.equal?(fr.rate, Decimal.new("0.0001"))
      assert fr.next_funding_time == next_funding
    end

    test "defaults version to 1" do
      assert %FundingRate{}.version == 1
    end
  end

  describe "version defaults" do
    test "all feed structs default to version 1" do
      assert %Ticker{}.version == 1
      assert %Trade{}.version == 1
      assert %BookSnapshot{}.version == 1
      assert %BookDelta{}.version == 1
      assert %Liquidation{}.version == 1
      assert %FundingRate{}.version == 1
    end
  end
end
