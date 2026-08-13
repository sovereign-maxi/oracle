defmodule Oracle.Aggregator do
  @moduledoc """
  Price aggregation strategies.

  Provides pure functions for aggregating prices from multiple sources
  using various strategies: median, mean, and VWAP.

  ## Example

      ticks = [
        %PriceTick{source: :binance, pair: :btc_usd, price: Decimal.new("100000"), ...},
        %PriceTick{source: :coinbase, pair: :btc_usd, price: Decimal.new("100010"), ...},
        %PriceTick{source: :kraken, pair: :btc_usd, price: Decimal.new("100005"), ...}
      ]

      {:ok, [%PriceUpdated{price: price}]} = Oracle.Aggregator.aggregate(ticks)
      # price = Decimal.new("100005")  # Median
  """

  alias Oracle.Events.{PriceStale, PriceTick, PriceUpdated, SourceOutlier}

  @default_outlier_threshold 5.0
  @default_max_swing_percent 10.0

  # ─────────────────────────────────────────────────────────────
  # Price Aggregation
  # ─────────────────────────────────────────────────────────────

  @doc """
  Processes a batch of price ticks into an aggregated price.

  ## Parameters

  - `ticks` - List of PriceTick events from different sources
  - `config` - Aggregation config:
    - `strategy` - :median | :mean | :vwap
    - `min_sources` - Minimum sources required
    - `outlier_threshold` - Percent deviation to flag as outlier (default 5.0)
    - `detect_outliers` - Whether to detect and report outliers (default false)
    - `prior_price` - Optional prior aggregated price (`Decimal.t()`) used
      to enforce the max-swing sanity floor. When absent, the swing check
      is skipped (first tick post-boot).
    - `max_swing_percent` - Optional percent deviation from `prior_price`
      above which a candidate aggregation is rejected as :max_swing_exceeded
      (default 10.0).

  ## Returns

  - `{:ok, [PriceUpdated | SourceOutlier]}` - Aggregated price + any outlier events
  - `{:error, :insufficient_sources, []}` - Not enough sources
  - `{:error, :empty_ticks, []}` - No ticks provided
  - `{:error, :mixed_pairs, []}` - Ticks for different pairs
  - `{:error, :negative_price, []}` - Negative price detected
  - `{:error, :zero_price, []}` - Zero price detected
  - `{:error, :max_swing_exceeded, []}` - Aggregated price deviates from
    `prior_price` by more than the swing cap (only when `prior_price` is
    supplied)

  ## Telemetry

  On rejection, emits `[:oracle, :aggregator, :rejected]` with
  measurements `%{count: 1}` and metadata
  `%{reason: :zero | :max_swing_exceeded | ...}`.
  """
  @spec aggregate([PriceTick.t()], map()) :: {:ok, [struct()]} | {:error, atom(), list()}
  def aggregate(ticks, config \\ %{strategy: :median, min_sources: 2})

  def aggregate([], _config) do
    {:error, :empty_ticks, []}
  end

  def aggregate(ticks, config) do
    min_sources = Map.get(config, :min_sources, 2)

    if length(ticks) < min_sources do
      {:error, :insufficient_sources, []}
    else
      validate_and_aggregate(ticks, config)
    end
  end

  defp validate_and_aggregate(ticks, config) do
    pairs = ticks |> Enum.map(& &1.pair) |> Enum.uniq()

    if length(pairs) > 1 do
      {:error, :mixed_pairs, []}
    else
      do_validate_and_aggregate(ticks, config, hd(pairs))
    end
  end

  defp do_validate_and_aggregate(ticks, config, pair) do
    prices = Enum.map(ticks, & &1.price)

    cond do
      Enum.any?(prices, &(Decimal.compare(&1, Decimal.new(0)) == :lt)) ->
        emit_rejected(:negative_price, pair)
        {:error, :negative_price, []}

      Enum.any?(prices, &(Decimal.compare(&1, Decimal.new(0)) == :eq)) ->
        emit_rejected(:zero, pair)
        {:error, :zero_price, []}

      true ->
        aggregate_and_swing_check(ticks, config, pair)
    end
  end

  defp aggregate_and_swing_check(ticks, config, pair) do
    case do_aggregate(ticks, config) do
      {:ok, [%PriceUpdated{price: candidate} | _]} = ok ->
        case swing_check(candidate, config) do
          :ok ->
            ok

          {:error, :max_swing_exceeded} ->
            emit_rejected(:max_swing_exceeded, pair)
            {:error, :max_swing_exceeded, []}
        end

      other ->
        other
    end
  end

  # Rejects aggregations that swing by more than `max_swing_percent`
  # from `prior_price` (both from config). When `prior_price` is
  # nil / missing / non-positive we skip the check — first tick
  # post-boot has no basis for comparison.
  defp swing_check(_candidate, %{prior_price: nil}), do: :ok

  defp swing_check(%Decimal{} = candidate, config) do
    case Map.get(config, :prior_price) do
      nil ->
        :ok

      %Decimal{} = prior ->
        case Decimal.compare(prior, Decimal.new(0)) do
          :gt -> compare_swing(candidate, prior, config)
          _ -> :ok
        end

      _ ->
        :ok
    end
  end

  defp swing_check(_candidate, _config), do: :ok

  defp compare_swing(candidate, prior, config) do
    max_percent = Map.get(config, :max_swing_percent, @default_max_swing_percent)
    threshold_frac = Decimal.div(to_decimal(max_percent), Decimal.new(100))

    delta = Decimal.abs(Decimal.sub(candidate, prior))
    ratio = Decimal.div(delta, prior)

    if Decimal.compare(ratio, threshold_frac) == :gt do
      {:error, :max_swing_exceeded}
    else
      :ok
    end
  end

  defp emit_rejected(reason, pair) do
    :telemetry.execute(
      [:oracle, :aggregator, :rejected],
      %{count: 1},
      %{reason: reason, pair: pair}
    )
  rescue
    # :telemetry may not be installed in every consumer. Swallow rather
    # than crash the aggregation.
    _ -> :ok
  end

  defp do_aggregate(ticks, config) do
    prices = Enum.map(ticks, & &1.price)
    sources = Enum.map(ticks, & &1.source)
    provenances = preserve_provenances(ticks)
    pair = hd(ticks).pair
    timestamp = latest_timestamp(ticks)

    strategy = Map.get(config, :strategy, :median)

    aggregated =
      case strategy do
        :median -> median(prices)
        :mean -> mean(prices)
        :vwap -> vwap(ticks)
      end

    price_event = %PriceUpdated{
      pair: pair,
      price: aggregated,
      sources: sources,
      timestamp: timestamp,
      provenances: provenances,
      version: 1
    }

    outlier_events =
      if Map.get(config, :detect_outliers, false) do
        threshold = Map.get(config, :outlier_threshold, @default_outlier_threshold)
        median_price = median(prices)
        detect_outliers(ticks, median_price, threshold, timestamp)
      else
        []
      end

    {:ok, [price_event | outlier_events]}
  end

  # Preserve per-tick provenance in aggregation output. If every tick lacks
  # provenance the returned aggregation has `nil` (indistinguishable from an
  # unsigned-feed aggregation). Any tick carrying provenance flips the list on
  # for the whole aggregation so consumers see the full per-source slice,
  # using `:absent` as a placeholder for un-signed sources (kept
  # ordering-aligned with `sources`).
  defp preserve_provenances(ticks) do
    any_present? = Enum.any?(ticks, &(&1.provenance != nil))

    if any_present? do
      Enum.map(ticks, fn tick -> tick.provenance || %{kind: :absent} end)
    else
      nil
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Aggregation Strategies
  # ─────────────────────────────────────────────────────────────

  @doc """
  Calculate median of prices.

  ## Examples

      iex> Oracle.Aggregator.median([Decimal.new(100), Decimal.new(200), Decimal.new(300)])
      Decimal.new(200)

      iex> Oracle.Aggregator.median([Decimal.new(100), Decimal.new(200)])
      Decimal.new(150)
  """
  @spec median([Decimal.t()]) :: Decimal.t()
  def median(prices) do
    sorted = Enum.sort(prices, &(Decimal.compare(&1, &2) != :gt))
    len = length(sorted)
    mid = div(len, 2)

    if rem(len, 2) == 0 do
      mid_1 = Enum.at(sorted, mid - 1)
      mid_2 = Enum.at(sorted, mid)
      Decimal.div(Decimal.add(mid_1, mid_2), 2)
    else
      Enum.at(sorted, mid)
    end
  end

  @doc """
  Calculate mean of prices.

  ## Examples

      iex> Oracle.Aggregator.mean([Decimal.new(100), Decimal.new(200), Decimal.new(300)])
      Decimal.new(200)
  """
  @spec mean([Decimal.t()]) :: Decimal.t()
  def mean([]), do: Decimal.new(0)

  def mean(prices) do
    sum = Enum.reduce(prices, Decimal.new(0), &Decimal.add/2)
    Decimal.div(sum, length(prices))
  end

  @doc """
  Calculate volume-weighted average price.

  Requires ticks to have non-nil volume. Falls back to mean if no volume data.

  ## Examples

      # With volume: VWAP = sum(price * volume) / sum(volume)
      # Without volume: Falls back to mean
  """
  @spec vwap([PriceTick.t()]) :: Decimal.t()
  def vwap(ticks) do
    # Only non-negative integer volumes are usable weights (the PriceTick
    # contract). Anything else is excluded rather than crashing the
    # aggregation or skewing it with a bad weight.
    ticks_with_volume =
      Enum.filter(ticks, fn tick ->
        is_integer(tick.volume) and tick.volume >= 0
      end)

    if Enum.empty?(ticks_with_volume) do
      mean(Enum.map(ticks, & &1.price))
    else
      {price_volume_sum, volume_sum} =
        Enum.reduce(ticks_with_volume, {Decimal.new(0), Decimal.new(0)}, fn tick, {pv, v} ->
          volume = Decimal.new(tick.volume)

          {
            Decimal.add(pv, Decimal.mult(tick.price, volume)),
            Decimal.add(v, volume)
          }
        end)

      if Decimal.compare(volume_sum, Decimal.new(0)) == :gt do
        Decimal.div(price_volume_sum, volume_sum)
      else
        mean(Enum.map(ticks, & &1.price))
      end
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Outlier Detection
  # ─────────────────────────────────────────────────────────────

  @doc """
  Detects price outliers based on deviation from median.

  Returns SourceOutlier events for any source > threshold% from median.
  """
  @spec detect_outliers([PriceTick.t()], Decimal.t(), number(), DateTime.t()) :: [SourceOutlier.t()]
  def detect_outliers(ticks, median_price, threshold_percent, timestamp) do
    threshold_decimal = Decimal.div(to_decimal(threshold_percent), Decimal.new(100))

    ticks
    |> Enum.filter(fn tick ->
      deviation = calculate_deviation(tick.price, median_price)
      Decimal.compare(Decimal.abs(deviation), threshold_decimal) == :gt
    end)
    |> Enum.map(fn tick ->
      deviation = calculate_deviation(tick.price, median_price)
      deviation_percent = Decimal.mult(deviation, Decimal.new(100)) |> Decimal.to_float()

      %SourceOutlier{
        source: tick.source,
        pair: tick.pair,
        price: tick.price,
        median_price: median_price,
        deviation_percent: deviation_percent,
        timestamp: timestamp,
        version: 1
      }
    end)
  end

  defp calculate_deviation(price, median) do
    if Decimal.equal?(median, Decimal.new(0)) do
      Decimal.new(0)
    else
      Decimal.div(Decimal.sub(price, median), median)
    end
  end

  defp to_decimal(value) when is_integer(value), do: Decimal.new(value)
  defp to_decimal(value) when is_float(value), do: Decimal.from_float(value)
  defp to_decimal(%Decimal{} = value), do: value

  defp latest_timestamp(ticks) do
    ticks
    |> Enum.map(& &1.timestamp)
    |> Enum.max(DateTime)
  end

  # ─────────────────────────────────────────────────────────────
  # Staleness Detection
  # ─────────────────────────────────────────────────────────────

  @doc """
  Checks if a price is stale (hasn't updated within threshold).

  ## Parameters

  - `pair` - The trading pair
  - `last_update` - DateTime of last price update
  - `threshold_seconds` - Staleness threshold in seconds (default 30)
  - `now` - Current DateTime (default utc_now)

  ## Returns

  - `nil` - Price is fresh
  - `%PriceStale{}` - Price is stale
  """
  @spec check_staleness(atom(), DateTime.t(), pos_integer(), DateTime.t()) :: PriceStale.t() | nil
  def check_staleness(pair, last_update, threshold_seconds \\ 30, now \\ DateTime.utc_now()) do
    age_seconds = DateTime.diff(now, last_update, :second)

    if age_seconds > threshold_seconds do
      %PriceStale{
        pair: pair,
        last_update: last_update,
        threshold_seconds: threshold_seconds,
        timestamp: now,
        version: 1
      }
    else
      nil
    end
  end

  @doc """
  Checks staleness for multiple pairs.

  Returns list of PriceStale events for any stale pairs.
  """
  @spec check_all_staleness(%{atom() => DateTime.t()}, pos_integer(), DateTime.t()) ::
          [PriceStale.t()]
  def check_all_staleness(last_updates, threshold_seconds \\ 30, now \\ DateTime.utc_now()) do
    last_updates
    |> Enum.map(fn {pair, last_update} ->
      check_staleness(pair, last_update, threshold_seconds, now)
    end)
    |> Enum.reject(&is_nil/1)
  end
end
