defmodule Oracle.CandlesTest do
  use ExUnit.Case, async: true

  alias Oracle.Candles
  alias Oracle.Events.{CandleClosed, CandleUpdated}

  describe "process_tick/6" do
    test "creates new candle on first tick" do
      timestamp = 1_704_067_200
      {:ok, events, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp, 10)

      assert [%CandleUpdated{} = event] = events
      assert event.pair == :btc_usd
      assert event.timeframe == :"1m"
      assert event.open == 100.0
      assert event.high == 100.0
      assert event.low == 100.0
      assert event.close == 100.0
      assert event.volume == 10

      assert candle.time == 1_704_067_200
      assert candle.open == 100.0
    end

    test "updates candle in same period" do
      timestamp1 = 1_704_067_200
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp1, 10)

      # 30 seconds later, same minute
      timestamp2 = 1_704_067_230
      {:ok, events, updated} = Candles.process_tick(:btc_usd, :"1m", candle, 105.0, timestamp2, 15)

      assert [%CandleUpdated{} = event] = events
      assert event.high == 105.0
      assert event.low == 100.0
      assert event.close == 105.0
      assert event.volume == 25

      assert updated.high == 105.0
      assert updated.volume == 25
    end

    test "closes candle and starts new one when period changes" do
      timestamp1 = 1_704_067_200
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp1, 10)

      # 60 seconds later, new minute
      timestamp2 = 1_704_067_260

      {:ok, events, new_candle} =
        Candles.process_tick(:btc_usd, :"1m", candle, 102.0, timestamp2, 5)

      assert [%CandleClosed{} = closed, %CandleUpdated{} = updated] = events

      # Closed candle should have old data
      assert closed.pair == :btc_usd
      assert closed.time == 1_704_067_200
      assert closed.close == 100.0

      # New candle should have new data
      assert updated.time == 1_704_067_260
      assert updated.open == 102.0

      assert new_candle.time == 1_704_067_260
    end

    test "tracks high correctly" do
      timestamp = 1_704_067_200
      {:ok, _, c1} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp)
      {:ok, _, c2} = Candles.process_tick(:btc_usd, :"1m", c1, 105.0, timestamp + 10)
      {:ok, _, c3} = Candles.process_tick(:btc_usd, :"1m", c2, 102.0, timestamp + 20)

      assert c3.high == 105.0
    end

    test "tracks low correctly" do
      timestamp = 1_704_067_200
      {:ok, _, c1} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp)
      {:ok, _, c2} = Candles.process_tick(:btc_usd, :"1m", c1, 95.0, timestamp + 10)
      {:ok, _, c3} = Candles.process_tick(:btc_usd, :"1m", c2, 98.0, timestamp + 20)

      assert c3.low == 95.0
    end

    test "handles different timeframes" do
      timestamp = 1_704_067_200

      {:ok, [event_1m], _} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp)
      {:ok, [event_5m], _} = Candles.process_tick(:btc_usd, :"5m", nil, 100.0, timestamp)
      {:ok, [event_1h], _} = Candles.process_tick(:btc_usd, :"1h", nil, 100.0, timestamp)

      assert event_1m.timeframe == :"1m"
      assert event_5m.timeframe == :"5m"
      assert event_1h.timeframe == :"1h"
    end

    test "default volume is 0" do
      timestamp = 1_704_067_200
      {:ok, [event], candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp)

      assert event.volume == 0
      assert candle.volume == 0
    end

    test "ignores out-of-order ticks from closed periods" do
      timestamp = 1_704_067_200
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp, 10)

      # Tick from the previous minute arrives late (reconnect replay)
      assert {:ok, [], ^candle} =
               Candles.process_tick(:btc_usd, :"1m", candle, 95.0, timestamp - 30, 5)
    end
  end

  describe "update_candle/4" do
    test "creates new candle when nil" do
      candle = Candles.update_candle(nil, 100.0, 1_704_067_200, 10)

      assert candle.time == 1_704_067_200
      assert candle.open == 100.0
      assert candle.high == 100.0
      assert candle.low == 100.0
      assert candle.close == 100.0
      assert candle.volume == 10
    end

    test "updates existing candle" do
      initial = %{
        time: 1_704_067_200,
        open: 100.0,
        high: 100.0,
        low: 100.0,
        close: 100.0,
        volume: 10
      }

      updated = Candles.update_candle(initial, 105.0, 1_704_067_210, 15)

      assert updated.open == 100.0
      assert updated.high == 105.0
      assert updated.low == 100.0
      assert updated.close == 105.0
      assert updated.volume == 25
    end

    test "updates low when price drops" do
      initial = %{
        time: 1_704_067_200,
        open: 100.0,
        high: 100.0,
        low: 100.0,
        close: 100.0,
        volume: 10
      }

      updated = Candles.update_candle(initial, 95.0, 1_704_067_210, 5)

      assert updated.low == 95.0
      assert updated.close == 95.0
    end
  end

  describe "aggregate_timeframe/2" do
    test "aggregates 1-minute candles into 5-minute" do
      candles = [
        %{time: 0, open: 100.0, high: 105.0, low: 99.0, close: 102.0, volume: 10},
        %{time: 60, open: 102.0, high: 108.0, low: 101.0, close: 107.0, volume: 15},
        %{time: 120, open: 107.0, high: 110.0, low: 105.0, close: 109.0, volume: 12}
      ]

      result = Candles.aggregate_timeframe(candles, :"5m")

      assert length(result) == 1

      [aggregated] = result
      assert aggregated.time == 0
      assert aggregated.open == 100.0
      assert aggregated.high == 110.0
      assert aggregated.low == 99.0
      assert aggregated.close == 109.0
      assert aggregated.volume == 37
    end

    test "creates multiple candles for longer periods" do
      candles =
        0..9
        |> Enum.map(fn i ->
          %{
            time: i * 60,
            open: 100.0 + i,
            high: 105.0 + i,
            low: 95.0 + i,
            close: 100.0 + i,
            volume: 10
          }
        end)

      result = Candles.aggregate_timeframe(candles, :"5m")

      assert length(result) == 2

      [first, second] = result
      assert first.time == 0
      assert second.time == 300
    end

    test "preserves order" do
      candles = [
        %{time: 300, open: 110.0, high: 115.0, low: 108.0, close: 112.0, volume: 20},
        %{time: 0, open: 100.0, high: 105.0, low: 99.0, close: 102.0, volume: 10}
      ]

      result = Candles.aggregate_timeframe(candles, :"5m")

      assert length(result) == 2
      assert hd(result).time == 0
      assert List.last(result).time == 300
    end

    test "handles empty list" do
      assert Candles.aggregate_timeframe([], :"5m") == []
    end

    test "handles single candle" do
      candles = [%{time: 0, open: 100.0, high: 105.0, low: 99.0, close: 102.0, volume: 10}]
      result = Candles.aggregate_timeframe(candles, :"5m")

      assert length(result) == 1
      assert hd(result) == hd(candles)
    end
  end

  describe "timeframe_seconds/1" do
    test "returns correct seconds for each timeframe" do
      assert Candles.timeframe_seconds(:"1m") == 60
      assert Candles.timeframe_seconds(:"5m") == 300
      assert Candles.timeframe_seconds(:"15m") == 900
      assert Candles.timeframe_seconds(:"30m") == 1800
      assert Candles.timeframe_seconds(:"1h") == 3600
      assert Candles.timeframe_seconds(:"4h") == 14_400
    end
  end

  describe "candle boundary alignment" do
    test "aligns to minute boundaries" do
      # Tick at 1:30:45 should align to 1:30:00
      timestamp = 1_704_067_845
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 100.0, timestamp)

      # 1704067845 / 60 * 60 = 1704067800
      assert candle.time == 1_704_067_800
    end

    test "aligns to 5-minute boundaries" do
      # Tick at 1:33:00 should align to 1:30:00
      timestamp = 1_704_067_980
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"5m", nil, 100.0, timestamp)

      # 1704067980 / 300 * 300 = 1704067800
      assert candle.time == 1_704_067_800
    end

    test "aligns to hour boundaries" do
      # Tick at 1:45:00 should align to 1:00:00
      timestamp = 1_704_070_500
      {:ok, _, candle} = Candles.process_tick(:btc_usd, :"1h", nil, 100.0, timestamp)

      # 1704070500 / 3600 * 3600 = 1704067200
      assert candle.time == 1_704_067_200
    end
  end
end
