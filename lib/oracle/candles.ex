defmodule Oracle.Candles do
  @moduledoc """
  OHLC candle building from price ticks.

  Provides pure functions for building and aggregating OHLC (Open, High, Low, Close)
  candles from price tick data.

  ## Candle Structure

      %{
        time: integer(),    # Unix timestamp of candle start
        open: float(),      # Opening price
        high: float(),      # Highest price
        low: float(),       # Lowest price
        close: float(),     # Closing/current price
        volume: integer()   # Total volume
      }

  ## Supported Timeframes

  - `:"1m"` - 1 minute
  - `:"5m"` - 5 minutes
  - `:"15m"` - 15 minutes
  - `:"30m"` - 30 minutes
  - `:"1h"` - 1 hour
  - `:"4h"` - 4 hours
  """

  alias Oracle.Events.{CandleClosed, CandleUpdated}

  @type candle :: %{
          time: integer(),
          open: float(),
          high: float(),
          low: float(),
          close: float(),
          volume: non_neg_integer()
        }

  # ─────────────────────────────────────────────────────────────
  # Tick Processing
  # ─────────────────────────────────────────────────────────────

  @doc """
  Updates a candle with a new price tick, returning events.

  If the tick belongs to a new candle period, closes the previous
  candle and starts a new one.

  ## Parameters

  - `pair` - Trading pair
  - `timeframe` - Candle timeframe (:"1m", :"5m", etc.)
  - `current_candle` - Current open candle (or nil)
  - `price` - New price (float)
  - `timestamp` - Unix timestamp of tick
  - `volume` - Trade volume (optional, default 0)

  ## Returns

  - `{:ok, events, updated_candle}`
    - events may include CandleClosed (if period rolled) + CandleUpdated

  ## Examples

      # First tick starts a new candle
      {:ok, [%CandleUpdated{}], candle} =
        Oracle.Candles.process_tick(:btc_usd, :"1m", nil, 100.0, 1704067200)

      # Tick in same period updates the candle
      {:ok, [%CandleUpdated{}], candle} =
        Oracle.Candles.process_tick(:btc_usd, :"1m", candle, 101.0, 1704067230)

      # Tick in new period closes old, starts new
      {:ok, [%CandleClosed{}, %CandleUpdated{}], candle} =
        Oracle.Candles.process_tick(:btc_usd, :"1m", candle, 102.0, 1704067260)
  """
  @spec process_tick(atom(), atom(), candle() | nil, float(), integer(), non_neg_integer()) ::
          {:ok, [struct()], candle()}
  def process_tick(pair, timeframe, current_candle, price, timestamp, volume \\ 0) do
    interval = timeframe_seconds(timeframe)
    candle_time = div(timestamp, interval) * interval
    now = DateTime.from_unix!(timestamp)

    case current_candle do
      nil ->
        # First candle
        new_candle = %{
          time: candle_time,
          open: price,
          high: price,
          low: price,
          close: price,
          volume: volume
        }

        event = candle_updated_event(pair, timeframe, new_candle, now)
        {:ok, [event], new_candle}

      %{time: ^candle_time} ->
        # Same candle period - update
        updated = %{
          current_candle
          | high: max(current_candle.high, price),
            low: min(current_candle.low, price),
            close: price,
            volume: current_candle.volume + volume
        }

        event = candle_updated_event(pair, timeframe, updated, now)
        {:ok, [event], updated}

      %{time: old_time} when old_time < candle_time ->
        # New candle period - close old, start new
        closed_event = %CandleClosed{
          pair: pair,
          timeframe: timeframe,
          time: current_candle.time,
          open: current_candle.open,
          high: current_candle.high,
          low: current_candle.low,
          close: current_candle.close,
          volume: current_candle.volume,
          timestamp: now,
          version: 1
        }

        new_candle = %{
          time: candle_time,
          open: price,
          high: price,
          low: price,
          close: price,
          volume: volume
        }

        updated_event = candle_updated_event(pair, timeframe, new_candle, now)

        {:ok, [closed_event, updated_event], new_candle}
    end
  end

  defp candle_updated_event(pair, timeframe, candle, now) do
    %CandleUpdated{
      pair: pair,
      timeframe: timeframe,
      time: candle.time,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      timestamp: now,
      version: 1
    }
  end

  # ─────────────────────────────────────────────────────────────
  # Legacy Functions
  # ─────────────────────────────────────────────────────────────

  @doc """
  Updates a candle with a new price tick.
  Returns new or updated candle (legacy - no events).

  ## Examples

      iex> Oracle.Candles.update_candle(nil, 100.0, 1704067200, 10)
      %{time: 1704067200, open: 100.0, high: 100.0, low: 100.0, close: 100.0, volume: 10}
  """
  @spec update_candle(candle() | nil, float(), integer(), non_neg_integer()) :: candle()
  def update_candle(candle, price, timestamp, volume \\ 0)

  def update_candle(nil, price, timestamp, volume) do
    minute_key = div(timestamp, 60) * 60
    %{time: minute_key, open: price, high: price, low: price, close: price, volume: volume}
  end

  def update_candle(candle, price, _timestamp, volume) do
    %{
      candle
      | high: max(candle.high, price),
        low: min(candle.low, price),
        close: price,
        volume: candle.volume + volume
    }
  end

  # ─────────────────────────────────────────────────────────────
  # Timeframe Aggregation
  # ─────────────────────────────────────────────────────────────

  @doc """
  Aggregates 1-minute candles into larger timeframes.

  ## Examples

      iex> candles = [
      ...>   %{time: 0, open: 100, high: 105, low: 99, close: 102, volume: 10},
      ...>   %{time: 60, open: 102, high: 108, low: 101, close: 107, volume: 15},
      ...>   %{time: 120, open: 107, high: 110, low: 105, close: 109, volume: 12}
      ...> ]
      iex> Oracle.Candles.aggregate_timeframe(candles, :"5m")
      [%{time: 0, open: 100, high: 110, low: 99, close: 109, volume: 37}]
  """
  @spec aggregate_timeframe([candle()], atom()) :: [candle()]
  def aggregate_timeframe(candles, timeframe) do
    interval = timeframe_seconds(timeframe)

    candles
    |> Enum.group_by(fn c -> div(c.time, interval) * interval end)
    |> Enum.map(fn {bucket_time, bucket_candles} ->
      sorted = Enum.sort_by(bucket_candles, & &1.time)

      %{
        time: bucket_time,
        open: hd(sorted).open,
        high: Enum.max_by(sorted, & &1.high).high,
        low: Enum.min_by(sorted, & &1.low).low,
        close: List.last(sorted).close,
        volume: Enum.sum(Enum.map(sorted, & &1.volume))
      }
    end)
    |> Enum.sort_by(& &1.time)
  end

  # ─────────────────────────────────────────────────────────────
  # Timeframe Helpers
  # ─────────────────────────────────────────────────────────────

  @doc """
  Returns the number of seconds in a timeframe.

  ## Examples

      iex> Oracle.Candles.timeframe_seconds(:"1m")
      60

      iex> Oracle.Candles.timeframe_seconds(:"1h")
      3600
  """
  @spec timeframe_seconds(atom()) :: pos_integer()
  def timeframe_seconds(:"1m"), do: 60
  def timeframe_seconds(:"5m"), do: 300
  def timeframe_seconds(:"15m"), do: 900
  def timeframe_seconds(:"30m"), do: 1800
  def timeframe_seconds(:"1h"), do: 3600
  def timeframe_seconds(:"4h"), do: 14_400
end
