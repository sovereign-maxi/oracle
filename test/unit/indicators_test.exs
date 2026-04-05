defmodule Oracle.IndicatorsTest do
  use ExUnit.Case, async: true

  alias Oracle.Events.{IndicatorsRequested, IndicatorsUpdated}
  alias Oracle.Indicators
  alias Oracle.Test.Helpers

  describe "calculate/1" do
    test "calculates all indicators from candle data" do
      candles = Helpers.random_candles(50, base_price: 100.0)

      request = %IndicatorsRequested{
        pair: :btc_usd,
        timeframe: :"1m",
        periods: [5, 10, 20],
        candles: candles,
        timestamp: DateTime.utc_now(),
        version: 1
      }

      assert {:ok, [%IndicatorsUpdated{} = result]} = Indicators.calculate(request)

      assert result.pair == :btc_usd
      assert result.timeframe == :"1m"
      assert is_map(result.sma)
      assert is_map(result.ema)
      assert is_map(result.macd)
      assert is_map(result.bollinger)

      # SMA periods should match requested periods
      assert Map.keys(result.sma) |> Enum.sort() == [5, 10, 20]
      assert Map.keys(result.ema) |> Enum.sort() == [5, 10, 20]

      # MACD should have standard keys
      assert Map.keys(result.macd) |> Enum.sort() == [:histogram, :macd, :signal]

      # Bollinger should have bands
      assert Map.keys(result.bollinger) |> Enum.sort() == [:lower, :middle, :upper]
    end
  end

  describe "sma/2" do
    test "calculates simple moving average" do
      values = [1, 2, 3, 4, 5]
      result = Indicators.sma(values, 3)

      # [1,2,3] -> 2, [2,3,4] -> 3, [3,4,5] -> 4
      assert result == [2.0, 3.0, 4.0]
    end

    test "returns empty list when not enough values" do
      values = [1, 2]
      assert Indicators.sma(values, 3) == []
    end

    test "handles period of 1" do
      values = [10, 20, 30]
      assert Indicators.sma(values, 1) == [10.0, 20.0, 30.0]
    end

    test "calculates correctly for larger datasets" do
      values = Enum.to_list(1..100)
      result = Indicators.sma(values, 10)

      # Should have 91 values (100 - 10 + 1)
      assert length(result) == 91

      # First SMA should be average of 1..10 = 5.5
      assert hd(result) == 5.5

      # Last SMA should be average of 91..100 = 95.5
      assert List.last(result) == 95.5
    end
  end

  describe "ema/2" do
    test "calculates exponential moving average" do
      values = [22, 22, 21, 24, 24, 23, 25, 26, 20, 24]
      result = Indicators.ema(values, 5)

      # First value should be SMA of first 5: (22+22+21+24+24)/5 = 22.6
      assert Float.round(hd(result), 2) == 22.6

      # Should have 6 values (10 - 5 + 1)
      assert length(result) == 6
    end

    test "returns empty list when not enough values" do
      values = [1, 2, 3]
      assert Indicators.ema(values, 5) == []
    end

    test "ema of constant values equals that value" do
      values = [100, 100, 100, 100, 100, 100, 100]
      result = Indicators.ema(values, 3)

      Enum.each(result, fn v ->
        assert_in_delta(v, 100.0, 0.001)
      end)
    end
  end

  describe "macd/4" do
    test "calculates MACD with default parameters" do
      values = Enum.to_list(1..50)
      result = Indicators.macd(values)

      assert is_list(result.macd)
      assert is_list(result.signal)
      assert is_list(result.histogram)

      # All should be non-empty for 50 values
      assert result.macd != []
      assert result.signal != []
      assert result.histogram != []
    end

    test "returns empty maps when not enough values" do
      values = [1, 2, 3, 4, 5]
      result = Indicators.macd(values)

      assert result.macd == []
      assert result.signal == []
      assert result.histogram == []
    end

    test "histogram is difference between macd and signal" do
      values = Enum.map(1..60, fn _i -> 100.0 + :rand.uniform() * 10 end)
      result = Indicators.macd(values)

      # Histogram should equal MACD - Signal for aligned values
      macd_aligned = Enum.drop(result.macd, length(result.macd) - length(result.signal))

      Enum.zip([macd_aligned, result.signal, result.histogram])
      |> Enum.each(fn {m, s, h} ->
        assert_in_delta(h, m - s, 0.0001)
      end)
    end

    test "accepts custom parameters" do
      values = Enum.to_list(1..30)
      result = Indicators.macd(values, 5, 10, 3)

      # With smaller parameters, should work with fewer values
      assert result.macd != []
    end
  end

  describe "bollinger/3" do
    test "calculates Bollinger Bands" do
      values = Enum.to_list(1..25)
      result = Indicators.bollinger(values, 5, 2)

      assert is_list(result.upper)
      assert is_list(result.middle)
      assert is_list(result.lower)

      # All should have same length
      assert length(result.upper) == length(result.middle)
      assert length(result.middle) == length(result.lower)

      # Middle should equal SMA
      assert result.middle == Indicators.sma(values, 5)
    end

    test "returns empty maps when not enough values" do
      values = [1, 2, 3]
      result = Indicators.bollinger(values, 5, 2)

      assert result.upper == []
      assert result.middle == []
      assert result.lower == []
    end

    test "upper band > middle > lower band" do
      values = Enum.map(1..30, fn _i -> 100.0 + :rand.uniform() * 20 end)
      result = Indicators.bollinger(values, 10, 2)

      Enum.zip([result.upper, result.middle, result.lower])
      |> Enum.each(fn {u, m, l} ->
        assert u >= m, "upper #{u} should be >= middle #{m}"
        assert m >= l, "middle #{m} should be >= lower #{l}"
      end)
    end

    test "bands converge when values are constant" do
      values = List.duplicate(100.0, 30)
      result = Indicators.bollinger(values, 10, 2)

      # With constant values, std dev is 0, so all bands equal the mean
      Enum.zip([result.upper, result.middle, result.lower])
      |> Enum.each(fn {u, m, l} ->
        assert_in_delta(u, 100.0, 0.001)
        assert_in_delta(m, 100.0, 0.001)
        assert_in_delta(l, 100.0, 0.001)
      end)
    end

    test "std_dev multiplier affects band width" do
      values = Enum.to_list(1..30)

      result_1 = Indicators.bollinger(values, 10, 1)
      result_2 = Indicators.bollinger(values, 10, 2)

      # Bands with multiplier 2 should be wider than multiplier 1
      Enum.zip([result_1.upper, result_2.upper, result_1.middle])
      |> Enum.each(fn {u1, u2, m} ->
        width_1 = u1 - m
        width_2 = u2 - m
        assert width_2 > width_1, "2x band width should be greater than 1x"
      end)
    end
  end

  describe "edge cases" do
    test "handles single value gracefully" do
      assert Indicators.sma([100], 1) == [100.0]
      assert Indicators.sma([100], 2) == []
      assert Indicators.ema([100], 1) == [100.0]
      assert Indicators.ema([100], 2) == []
    end

    test "handles negative values" do
      values = [-10, -5, 0, 5, 10]
      result = Indicators.sma(values, 3)
      assert result == [-5.0, 0.0, 5.0]
    end

    test "handles very large values" do
      values = [1.0e10, 2.0e10, 3.0e10, 4.0e10, 5.0e10]
      result = Indicators.sma(values, 3)
      assert result == [2.0e10, 3.0e10, 4.0e10]
    end
  end
end
