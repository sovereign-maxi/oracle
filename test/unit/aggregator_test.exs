defmodule Oracle.AggregatorTest do
  use ExUnit.Case, async: true

  alias Oracle.Aggregator
  alias Oracle.Events.{PriceStale, PriceTick, PriceUpdated, SourceOutlier}
  alias Oracle.Test.Helpers

  describe "aggregate/2" do
    test "aggregates prices using median (odd number of sources)" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100000"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("100010"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("100005"),
          timestamp: timestamp
        }
      ]

      assert {:ok, [%PriceUpdated{price: price, pair: :btc_usd, sources: sources}]} =
               Aggregator.aggregate(ticks)

      # Median of [100000, 100005, 100010] = 100005
      assert Decimal.equal?(price, Decimal.new("100005"))
      assert length(sources) == 3
      assert :binance in sources
      assert :coinbase in sources
      assert :kraken in sources
    end

    test "aggregates prices using median (even number of sources)" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          timestamp: timestamp
        }
      ]

      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks)
      # Median of [100, 200] = 150
      assert Decimal.equal?(price, Decimal.new("150"))
    end

    test "aggregates prices using mean" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("300"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :mean, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)
      # Mean of [100, 200, 300] = 200
      assert Decimal.equal?(price, Decimal.new("200"))
    end

    test "fails with empty ticks" do
      assert {:error, :empty_ticks, []} = Aggregator.aggregate([])
    end

    test "fails with insufficient sources" do
      tick =
        Helpers.random_price_tick(source: :binance, pair: :btc_usd, price: Decimal.new("100000"))

      assert {:error, :insufficient_sources, []} = Aggregator.aggregate([tick])
    end

    test "fails with mixed pairs" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100000"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :eth_usd,
          price: Decimal.new("3000"),
          timestamp: timestamp
        }
      ]

      assert {:error, :mixed_pairs, []} = Aggregator.aggregate(ticks)
    end

    test "fails with negative price" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100000"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("-100"),
          timestamp: timestamp
        }
      ]

      assert {:error, :negative_price, []} = Aggregator.aggregate(ticks)
    end

    test "fails with zero price" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100000"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new(0),
          timestamp: timestamp
        }
      ]

      assert {:error, :zero_price, []} = Aggregator.aggregate(ticks)
    end

    test "emits [:oracle, :aggregator, :rejected] telemetry with :zero reason" do
      handler = "aggregator-zero-#{System.unique_integer([:positive])}"
      test_pid = self()
      ref = make_ref()

      :telemetry.attach(
        handler,
        [:oracle, :aggregator, :rejected],
        fn _evt, measurements, metadata, _ ->
          send(test_pid, {ref, measurements, metadata})
        end,
        nil
      )

      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{source: :binance, pair: :btc_usd, price: Decimal.new(0), timestamp: timestamp},
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new(0),
          timestamp: timestamp
        }
      ]

      assert {:error, :zero_price, []} = Aggregator.aggregate(ticks)

      assert_receive {^ref, %{count: 1}, %{reason: :zero, pair: :btc_usd}}, 500

      :telemetry.detach(handler)
    end

    test "rejects :max_swing_exceeded when candidate deviates > swing cap from prior" do
      handler = "aggregator-swing-#{System.unique_integer([:positive])}"
      test_pid = self()
      ref = make_ref()

      :telemetry.attach(
        handler,
        [:oracle, :aggregator, :rejected],
        fn _evt, measurements, metadata, _ ->
          send(test_pid, {ref, measurements, metadata})
        end,
        nil
      )

      timestamp = DateTime.utc_now()

      # Prior 200, candidate median 100 = 50% swing, above default 10%.
      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, prior_price: Decimal.new("200")}

      assert {:error, :max_swing_exceeded, []} = Aggregator.aggregate(ticks, config)

      assert_receive {^ref, %{count: 1}, %{reason: :max_swing_exceeded, pair: :btc_usd}}, 500

      :telemetry.detach(handler)
    end

    test "accepts candidate within swing cap" do
      timestamp = DateTime.utc_now()

      # Prior 200, candidate 210 = 5% swing, below default 10%.
      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("210"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("210"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, prior_price: Decimal.new("200")}

      assert {:ok, [%PriceUpdated{}]} = Aggregator.aggregate(ticks, config)
    end

    test "skips swing check when prior_price is nil (first tick post-boot)" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, prior_price: nil}

      assert {:ok, [%PriceUpdated{}]} = Aggregator.aggregate(ticks, config)
    end

    test "honours override :max_swing_percent" do
      timestamp = DateTime.utc_now()

      # 5% swing candidate, override cap to 1% → reject.
      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("210"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("210"),
          timestamp: timestamp
        }
      ]

      config = %{
        strategy: :median,
        min_sources: 2,
        prior_price: Decimal.new("200"),
        max_swing_percent: 1.0
      }

      assert {:error, :max_swing_exceeded, []} = Aggregator.aggregate(ticks, config)
    end

    test "uses latest timestamp from ticks" do
      old_time = ~U[2026-01-01 00:00:00Z]
      new_time = ~U[2026-01-01 00:00:05Z]

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: old_time
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          timestamp: new_time
        }
      ]

      assert {:ok, [%PriceUpdated{timestamp: timestamp}]} = Aggregator.aggregate(ticks)
      assert timestamp == new_time
    end
  end

  describe "aggregate/2 with VWAP" do
    test "calculates VWAP with volume data" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          volume: 10,
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("110"),
          volume: 20,
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("105"),
          volume: 10,
          timestamp: timestamp
        }
      ]

      config = %{strategy: :vwap, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)

      # VWAP = (100*10 + 110*20 + 105*10) / (10 + 20 + 10) = 4250 / 40 = 106.25
      assert Decimal.equal?(price, Decimal.new("106.25"))
    end

    test "VWAP falls back to mean when no volume data" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          volume: nil,
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("110"),
          volume: nil,
          timestamp: timestamp
        }
      ]

      config = %{strategy: :vwap, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)

      # Falls back to mean: (100 + 110) / 2 = 105
      assert Decimal.equal?(price, Decimal.new("105"))
    end

    test "VWAP uses only ticks with volume when mixed" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          volume: 10,
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          volume: nil,
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("120"),
          volume: 10,
          timestamp: timestamp
        }
      ]

      config = %{strategy: :vwap, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)

      # VWAP only uses binance and kraken: (100*10 + 120*10) / 20 = 110
      assert Decimal.equal?(price, Decimal.new("110"))
    end

    test "VWAP ignores ticks with non-integer or negative volumes" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          volume: 10,
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          volume: 10.5,
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("50"),
          volume: -5,
          timestamp: timestamp
        }
      ]

      config = %{strategy: :vwap, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)

      # Only binance's volume is a usable weight
      assert Decimal.equal?(price, Decimal.new("100"))
    end

    test "VWAP falls back to mean when all volumes are unusable" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          volume: 1.5,
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("200"),
          volume: -3,
          timestamp: timestamp
        }
      ]

      config = %{strategy: :vwap, min_sources: 2}
      assert {:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks, config)

      assert Decimal.equal?(price, Decimal.new("150"))
    end
  end

  describe "aggregate/2 with outlier detection" do
    test "detects outliers when enabled" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("120"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, detect_outliers: true, outlier_threshold: 10.0}

      assert {:ok, [%PriceUpdated{}, %SourceOutlier{source: :kraken, deviation_percent: deviation}]} =
               Aggregator.aggregate(ticks, config)

      # Kraken is 20% above median of 100
      assert deviation == 20.0
    end

    test "no outlier events when detection disabled" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("150"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, detect_outliers: false}
      assert {:ok, [%PriceUpdated{}]} = Aggregator.aggregate(ticks, config)
    end

    test "no outlier events when all within threshold" do
      timestamp = DateTime.utc_now()

      ticks = [
        %PriceTick{
          source: :binance,
          pair: :btc_usd,
          price: Decimal.new("100"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :coinbase,
          pair: :btc_usd,
          price: Decimal.new("102"),
          timestamp: timestamp
        },
        %PriceTick{
          source: :kraken,
          pair: :btc_usd,
          price: Decimal.new("101"),
          timestamp: timestamp
        }
      ]

      config = %{strategy: :median, min_sources: 2, detect_outliers: true, outlier_threshold: 5.0}
      assert {:ok, [%PriceUpdated{}]} = Aggregator.aggregate(ticks, config)
    end
  end

  describe "median/1" do
    test "calculates median for odd count" do
      prices = [Decimal.new(1), Decimal.new(5), Decimal.new(3)]
      assert Decimal.equal?(Aggregator.median(prices), Decimal.new(3))
    end

    test "calculates median for even count" do
      prices = [Decimal.new(1), Decimal.new(2), Decimal.new(3), Decimal.new(4)]
      # Median of sorted [1, 2, 3, 4] = (2 + 3) / 2 = 2.5
      assert Decimal.equal?(Aggregator.median(prices), Decimal.new("2.5"))
    end

    test "handles single value" do
      prices = [Decimal.new(42)]
      assert Decimal.equal?(Aggregator.median(prices), Decimal.new(42))
    end
  end

  describe "mean/1" do
    test "calculates mean" do
      prices = [Decimal.new(10), Decimal.new(20), Decimal.new(30)]
      assert Decimal.equal?(Aggregator.mean(prices), Decimal.new(20))
    end
  end

  describe "check_staleness/4" do
    test "returns nil when price is fresh" do
      last_update = DateTime.utc_now()
      now = DateTime.add(last_update, 10, :second)

      assert is_nil(Aggregator.check_staleness(:btc_usd, last_update, 30, now))
    end

    test "returns PriceStale when price is stale" do
      last_update = DateTime.utc_now()
      now = DateTime.add(last_update, 60, :second)

      result = Aggregator.check_staleness(:btc_usd, last_update, 30, now)
      assert %PriceStale{pair: :btc_usd, threshold_seconds: 30} = result
      assert result.last_update == last_update
    end

    test "uses default threshold of 30 seconds" do
      last_update = DateTime.utc_now()
      now = DateTime.add(last_update, 31, :second)

      assert %PriceStale{} = Aggregator.check_staleness(:btc_usd, last_update, 30, now)
    end
  end

  describe "check_all_staleness/3" do
    test "returns stale events for multiple pairs" do
      now = DateTime.utc_now()
      fresh_update = DateTime.add(now, -10, :second)
      stale_update = DateTime.add(now, -60, :second)

      last_updates = %{
        btc_usd: stale_update,
        eth_usd: fresh_update,
        xau_usd: stale_update
      }

      results = Aggregator.check_all_staleness(last_updates, 30, now)

      assert length(results) == 2
      pairs = Enum.map(results, & &1.pair)
      assert :btc_usd in pairs
      assert :xau_usd in pairs
      refute :eth_usd in pairs
    end

    test "returns empty list when all fresh" do
      now = DateTime.utc_now()
      fresh_update = DateTime.add(now, -10, :second)

      last_updates = %{btc_usd: fresh_update, eth_usd: fresh_update}

      assert [] = Aggregator.check_all_staleness(last_updates, 30, now)
    end
  end

  describe "with random test data" do
    test "aggregates random price ticks" do
      ticks = Helpers.random_price_ticks(pair: :btc_usd, sources: [:binance, :coinbase, :kraken])

      assert {:ok, [%PriceUpdated{pair: :btc_usd}]} = Aggregator.aggregate(ticks)
    end

    test "aggregates single source when min_sources is 1" do
      tick = Helpers.random_price_tick(source: :binance, pair: :eth_usd)
      config = %{strategy: :median, min_sources: 1}

      assert {:ok, [%PriceUpdated{pair: :eth_usd, sources: [:binance]}]} =
               Aggregator.aggregate([tick], config)
    end
  end
end
