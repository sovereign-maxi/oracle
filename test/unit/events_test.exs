defmodule Oracle.EventsTest do
  use ExUnit.Case, async: true

  alias Oracle.Events.{
    CandleClosed,
    CandleUpdated,
    DerivedPriceUpdated,
    IndicatorsRequested,
    IndicatorsUpdated,
    PriceStale,
    PriceTick,
    PriceUpdated,
    SourceFailed,
    SourceOutlier,
    StreamConnected,
    StreamDisconnected,
    StreamError,
    StreamHealthChanged
  }

  describe "PriceTick" do
    test "has expected struct fields" do
      tick = %PriceTick{
        source: :binance,
        pair: :btc_usd,
        price: Decimal.new("100000"),
        volume: 100,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert tick.source == :binance
      assert tick.pair == :btc_usd
      assert tick.volume == 100
      assert tick.version == 1
    end

    test "defaults version to 1" do
      tick = %PriceTick{}
      assert tick.version == 1
    end

    test "volume defaults to nil" do
      tick = %PriceTick{source: :test, pair: :btc_usd, price: Decimal.new(1)}
      assert is_nil(tick.volume)
    end
  end

  describe "PriceUpdated" do
    test "has expected struct fields" do
      event = %PriceUpdated{
        pair: :btc_usd,
        price: Decimal.new("100000"),
        sources: [:binance, :coinbase],
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.pair == :btc_usd
      assert length(event.sources) == 2
      assert event.version == 1
    end
  end

  describe "DerivedPriceUpdated" do
    test "has formula field" do
      event = %DerivedPriceUpdated{
        pair: :btc_xau,
        price: Decimal.new("40"),
        base_pair: :btc_usd,
        quote_pair: :xau_usd,
        formula: :divide,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.formula == :divide
      assert event.base_pair == :btc_usd
      assert event.quote_pair == :xau_usd
    end
  end

  describe "IndicatorsRequested" do
    test "has candles and periods" do
      candles = [%{close: 100}, %{close: 101}]

      event = %IndicatorsRequested{
        pair: :btc_usd,
        timeframe: :"1m",
        periods: [5, 10, 20],
        candles: candles,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.periods == [5, 10, 20]
      assert length(event.candles) == 2
    end
  end

  describe "IndicatorsUpdated" do
    test "has indicator maps" do
      event = %IndicatorsUpdated{
        pair: :btc_usd,
        timeframe: :"1m",
        sma: %{5 => [1.0, 2.0], 10 => [1.5]},
        ema: %{5 => [1.1, 2.1]},
        macd: %{macd: [0.1], signal: [0.05], histogram: [0.05]},
        bollinger: %{upper: [102.0], middle: [100.0], lower: [98.0]},
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert is_map(event.sma)
      assert is_map(event.ema)
      assert is_map(event.macd)
      assert is_map(event.bollinger)
    end
  end

  describe "PriceStale" do
    test "has staleness info" do
      last_update = DateTime.utc_now()

      event = %PriceStale{
        pair: :btc_usd,
        last_update: last_update,
        threshold_seconds: 30,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.pair == :btc_usd
      assert event.threshold_seconds == 30
      assert event.last_update == last_update
    end
  end

  describe "SourceFailed" do
    test "has failure info" do
      event = %SourceFailed{
        source: :binance,
        pair: :btc_usd,
        reason: {:http_error, 500},
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.source == :binance
      assert event.reason == {:http_error, 500}
    end
  end

  describe "SourceOutlier" do
    test "has outlier info" do
      event = %SourceOutlier{
        source: :sketchy_exchange,
        pair: :btc_usd,
        price: Decimal.new("150000"),
        median_price: Decimal.new("100000"),
        deviation_percent: 50.0,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.deviation_percent == 50.0
      assert Decimal.equal?(event.median_price, Decimal.new("100000"))
    end
  end

  describe "CandleClosed" do
    test "has OHLCV data" do
      event = %CandleClosed{
        pair: :btc_usd,
        timeframe: :"1m",
        time: 1_704_067_200,
        open: 100.0,
        high: 105.0,
        low: 99.0,
        close: 102.0,
        volume: 1000,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.open == 100.0
      assert event.high == 105.0
      assert event.low == 99.0
      assert event.close == 102.0
      assert event.volume == 1000
    end
  end

  describe "CandleUpdated" do
    test "has same structure as CandleClosed" do
      event = %CandleUpdated{
        pair: :btc_usd,
        timeframe: :"1m",
        time: 1_704_067_200,
        open: 100.0,
        high: 105.0,
        low: 99.0,
        close: 104.0,
        volume: 500,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.timeframe == :"1m"
      assert event.close == 104.0
    end
  end

  describe "StreamConnected" do
    test "has expected struct fields" do
      event = %StreamConnected{
        source: :binance,
        channels: [%{feed: :ticker, pair: :btc_usdt}],
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.source == :binance
      assert length(event.channels) == 1
    end
  end

  describe "StreamDisconnected" do
    test "has expected struct fields" do
      event = %StreamDisconnected{
        source: :bybit,
        reason: :closed,
        attempt: 3,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.source == :bybit
      assert event.reason == :closed
      assert event.attempt == 3
    end
  end

  describe "StreamError" do
    test "has expected struct fields" do
      event = %StreamError{
        source: :okx,
        reason: {:parse_error, "invalid JSON"},
        raw_message: "{bad",
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.source == :okx
      assert event.raw_message == "{bad"
    end
  end

  describe "StreamHealthChanged" do
    test "has expected struct fields" do
      event = %StreamHealthChanged{
        source: :deribit,
        status: :degraded,
        metrics: %{message_rate: 0.5, last_message_age_ms: 35_000},
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert event.source == :deribit
      assert event.status == :degraded
      assert is_map(event.metrics)
    end
  end

  describe "version defaults" do
    test "all events default to version 1" do
      assert %PriceTick{}.version == 1
      assert %PriceUpdated{}.version == 1
      assert %DerivedPriceUpdated{}.version == 1
      assert %IndicatorsRequested{}.version == 1
      assert %IndicatorsUpdated{}.version == 1
      assert %PriceStale{}.version == 1
      assert %SourceFailed{}.version == 1
      assert %SourceOutlier{}.version == 1
      assert %CandleClosed{}.version == 1
      assert %CandleUpdated{}.version == 1
      assert %StreamConnected{}.version == 1
      assert %StreamDisconnected{}.version == 1
      assert %StreamError{}.version == 1
      assert %StreamHealthChanged{}.version == 1
    end
  end
end
