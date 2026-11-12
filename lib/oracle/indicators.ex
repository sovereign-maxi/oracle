defmodule Oracle.Indicators do
  @moduledoc """
  Technical indicator calculations.

  Provides pure functions for calculating common technical indicators:

  - **SMA** - Simple Moving Average
  - **EMA** - Exponential Moving Average
  - **MACD** - Moving Average Convergence Divergence
  - **Bollinger Bands** - Volatility bands around SMA
  - **RSI** - Relative Strength Index with Wilder smoothing

  Indicator functions operate on lists of numeric values. `align/2`
  attaches those values to the trailing candles that produced them.
  """

  alias Oracle.Events.{IndicatorsRequested, IndicatorsUpdated}

  @doc """
  Calculate indicators from candle data.

  ## Parameters

  - `event` - IndicatorsRequested event containing candles and configuration

  ## Returns

  - `{:ok, [IndicatorsUpdated]}`
  """
  @spec calculate(IndicatorsRequested.t()) :: {:ok, [IndicatorsUpdated.t()]}
  def calculate(%IndicatorsRequested{} = event) do
    closes = Enum.map(event.candles, & &1.close)

    indicators = %IndicatorsUpdated{
      pair: event.pair,
      timeframe: event.timeframe,
      sma: calculate_smas(closes, event.periods),
      ema: calculate_emas(closes, event.periods),
      macd: calculate_macd(closes),
      bollinger: calculate_bollinger(closes, 20, 2),
      timestamp: event.timestamp,
      version: 1
    }

    {:ok, [indicators]}
  end

  # ─────────────────────────────────────────────────────────────
  # SMA - Simple Moving Average
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculate Simple Moving Average.

  Returns a list of SMA values. The list will be shorter than the input
  by (period - 1) elements, as we need `period` values to calculate each SMA.

  ## Examples

      iex> Oracle.Indicators.sma([1, 2, 3, 4, 5], 3)
      [2.0, 3.0, 4.0]

      iex> Oracle.Indicators.sma([1, 2], 3)
      []
  """
  @spec sma([number()], pos_integer()) :: [float()]
  def sma(values, period) when length(values) < period, do: []

  def sma(values, period) do
    values
    |> Enum.chunk_every(period, 1, :discard)
    |> Enum.map(&(Enum.sum(&1) / period))
  end

  defp calculate_smas(closes, periods) do
    Map.new(periods, fn period -> {period, sma(closes, period)} end)
  end

  # ─────────────────────────────────────────────────────────────
  # EMA - Exponential Moving Average
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculate Exponential Moving Average.

  Uses the formula: EMA = (Price - Previous EMA) * Multiplier + Previous EMA
  where Multiplier = 2 / (period + 1)

  The first EMA value is calculated as an SMA of the first `period` values.

  ## Examples

      iex> values = [22, 22, 21, 24, 24, 23, 25, 26, 20, 24]
      iex> Oracle.Indicators.ema(values, 5) |> Enum.map(&Float.round(&1, 2))
      [22.6, 22.73, 23.49, 24.33, 22.89, 23.26]
  """
  @spec ema([number()], pos_integer()) :: [float()]
  def ema(values, period) when length(values) < period, do: []

  def ema(values, period) do
    multiplier = 2 / (period + 1)
    {initial, rest} = Enum.split(values, period)
    initial_sma = Enum.sum(initial) / period

    rest
    |> Enum.scan(initial_sma, fn price, prev_ema ->
      (price - prev_ema) * multiplier + prev_ema
    end)
    |> then(&[initial_sma | &1])
  end

  defp calculate_emas(closes, periods) do
    Map.new(periods, fn period -> {period, ema(closes, period)} end)
  end

  # ─────────────────────────────────────────────────────────────
  # MACD - Moving Average Convergence Divergence
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculate MACD (Moving Average Convergence Divergence).

  Returns a map with:
  - `macd` - MACD line (fast EMA - slow EMA)
  - `signal` - Signal line (EMA of MACD line)
  - `histogram` - MACD - Signal

  Default parameters: fast=12, slow=26, signal=9

  ## Examples

      iex> values = Enum.to_list(1..50)
      iex> result = Oracle.Indicators.macd(values)
      iex> Map.keys(result)
      [:histogram, :macd, :signal]
  """
  @spec macd([number()], pos_integer(), pos_integer(), pos_integer()) :: map()
  def macd(values, fast \\ 12, slow \\ 26, signal \\ 9)

  def macd(values, _fast, slow, _signal) when length(values) < slow do
    %{macd: [], signal: [], histogram: []}
  end

  def macd(values, fast, slow, signal) do
    ema_fast = ema(values, fast)
    ema_slow = ema(values, slow)

    # Align lengths (fast EMA is longer than slow EMA)
    diff = length(ema_fast) - length(ema_slow)
    ema_fast_aligned = Enum.drop(ema_fast, diff)

    macd_line = Enum.zip_with(ema_fast_aligned, ema_slow, &(&1 - &2))
    signal_line = ema(macd_line, signal)

    # Align for histogram
    diff2 = length(macd_line) - length(signal_line)
    macd_aligned = Enum.drop(macd_line, diff2)

    histogram = Enum.zip_with(macd_aligned, signal_line, &(&1 - &2))

    %{macd: macd_line, signal: signal_line, histogram: histogram}
  end

  defp calculate_macd(closes), do: macd(closes)

  # ─────────────────────────────────────────────────────────────
  # Bollinger Bands
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculate Bollinger Bands.

  Returns a map with:
  - `upper` - Upper band (SMA + std_dev * multiplier)
  - `middle` - Middle band (SMA)
  - `lower` - Lower band (SMA - std_dev * multiplier)

  Default parameters: period=20, std_dev=2

  ## Examples

      iex> values = Enum.to_list(1..25)
      iex> result = Oracle.Indicators.bollinger(values, 5, 2)
      iex> Map.keys(result)
      [:lower, :middle, :upper]
      iex> length(result.middle)
      21
  """
  @spec bollinger([number()], pos_integer(), number()) :: map()
  def bollinger(values, period \\ 20, std_dev \\ 2)

  def bollinger(values, period, _std_dev) when length(values) < period do
    %{upper: [], middle: [], lower: []}
  end

  def bollinger(values, period, std_dev_mult) do
    middle = sma(values, period)

    {upper, lower} =
      values
      |> Enum.chunk_every(period, 1, :discard)
      |> Enum.zip(middle)
      |> Enum.map(fn {window, sma_val} ->
        std = standard_deviation(window)
        {sma_val + std * std_dev_mult, sma_val - std * std_dev_mult}
      end)
      |> Enum.unzip()

    %{upper: upper, middle: middle, lower: lower}
  end

  defp calculate_bollinger(closes, period, std_dev), do: bollinger(closes, period, std_dev)

  defp standard_deviation(values) do
    mean = Enum.sum(values) / length(values)
    variance = Enum.sum(Enum.map(values, fn v -> (v - mean) ** 2 end)) / length(values)
    :math.sqrt(variance)
  end

  # ─────────────────────────────────────────────────────────────
  # RSI - Relative Strength Index
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculates the Relative Strength Index using Wilder smoothing.

  The first value appears after `period` price changes, so an input of
  `n` prices produces `n - period` values. A flat window returns 50,
  a gain-only window returns 100, and a loss-only window returns 0.
  """
  @spec rsi([number()], pos_integer()) :: [float()]
  def rsi(values, period \\ 14)

  def rsi(values, period) when is_list(values) and is_integer(period) and period > 0 do
    if length(values) <= period do
      []
    else
      deltas = values |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)
      {initial, rest} = Enum.split(deltas, period)
      gain = initial |> Enum.map(&max(&1, 0)) |> Enum.sum() |> Kernel./(period)
      loss = initial |> Enum.map(&max(-&1, 0)) |> Enum.sum() |> Kernel./(period)

      {_gain, _loss, values} =
        Enum.reduce(rest, {gain, loss, [rsi_value(gain, loss)]}, fn delta, {g, l, acc} ->
          next_gain = (g * (period - 1) + max(delta, 0)) / period
          next_loss = (l * (period - 1) + max(-delta, 0)) / period
          {next_gain, next_loss, [rsi_value(next_gain, next_loss) | acc]}
        end)

      Enum.reverse(values)
    end
  end

  defp rsi_value(gain, loss) do
    cond do
      gain == 0 and loss == 0 -> 50.0
      loss == 0 -> 100.0
      gain == 0 -> 0.0
      true -> 100.0 - 100.0 / (1.0 + gain / loss)
    end
  end

  @doc """
  Aligns an indicator list to the trailing candles that produced it.

  Warm-up values are omitted by construction. Raises when the values
  cannot have come from the supplied candle series.
  """
  @spec align([%{time: integer()}], [number()]) :: [%{time: integer(), value: number()}]
  def align(candles, values) when is_list(candles) and is_list(values) do
    if length(values) > length(candles),
      do: raise(ArgumentError, "indicator series exceeds candles")

    candles
    |> Enum.drop(length(candles) - length(values))
    |> Enum.zip(values)
    |> Enum.map(fn {%{time: time}, value} -> %{time: time, value: value} end)
  end
end
