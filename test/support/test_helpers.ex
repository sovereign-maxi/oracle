defmodule Oracle.Test.Helpers do
  @moduledoc """
  Test helpers for generating Oracle test data using Faker.
  """

  alias Oracle.Events.{IndicatorsRequested, PriceTick}

  @doc """
  Generates a random price tick.
  """
  @spec random_price_tick(keyword()) :: PriceTick.t()
  def random_price_tick(opts \\ []) do
    %PriceTick{
      source: Keyword.get(opts, :source, random_source()),
      pair: Keyword.get(opts, :pair, :btc_usd),
      price: Keyword.get(opts, :price, random_price()),
      volume: Keyword.get(opts, :volume, nil),
      timestamp: Keyword.get(opts, :timestamp, DateTime.utc_now()),
      version: 1
    }
  end

  @doc """
  Generates multiple price ticks for the same pair from different sources.
  """
  @spec random_price_ticks(keyword()) :: [PriceTick.t()]
  def random_price_ticks(opts \\ []) do
    pair = Keyword.get(opts, :pair, :btc_usd)
    base_price = Keyword.get(opts, :base_price, random_price())
    sources = Keyword.get(opts, :sources, [:binance, :coinbase, :kraken])
    variance = Keyword.get(opts, :variance, 0.001)
    timestamp = Keyword.get(opts, :timestamp, DateTime.utc_now())

    Enum.map(sources, fn source ->
      # Add small random variance to base price
      price_variance =
        Decimal.mult(base_price, Decimal.from_float(variance * (:rand.uniform() - 0.5)))

      price = Decimal.add(base_price, price_variance)

      random_price_tick(
        source: source,
        pair: pair,
        price: price,
        timestamp: timestamp
      )
    end)
  end

  @doc """
  Generates a random price as Decimal.
  """
  @spec random_price() :: Decimal.t()
  def random_price do
    price = Faker.Commerce.price() * 1000
    Decimal.from_float(price)
  end

  @doc """
  Generates a specific price as Decimal.
  """
  @spec price(String.t() | number()) :: Decimal.t()
  def price(value) when is_binary(value), do: Decimal.new(value)
  def price(value) when is_integer(value), do: Decimal.new(value)
  def price(value) when is_float(value), do: Decimal.from_float(value)

  @doc """
  Generates a random source atom.
  """
  @spec random_source() :: atom()
  def random_source do
    Enum.random([:binance, :coinbase, :kraken, :bitstamp, :gemini])
  end

  @doc """
  Generates random OHLC candle data.
  """
  @spec random_candle(keyword()) :: map()
  def random_candle(opts \\ []) do
    base = Keyword.get(opts, :base_price, 100.0)
    time = Keyword.get(opts, :time, System.system_time(:second))
    volatility = Keyword.get(opts, :volatility, 0.02)

    open = base * (1 + (:rand.uniform() - 0.5) * volatility)
    close = base * (1 + (:rand.uniform() - 0.5) * volatility)
    high = max(open, close) * (1 + :rand.uniform() * volatility)
    low = min(open, close) * (1 - :rand.uniform() * volatility)

    %{
      time: time,
      open: open,
      high: high,
      low: low,
      close: close,
      volume: :rand.uniform(1000)
    }
  end

  @doc """
  Generates a list of sequential candles.
  """
  @spec random_candles(pos_integer(), keyword()) :: [map()]
  def random_candles(count, opts \\ []) do
    base_price = Keyword.get(opts, :base_price, 100.0)
    start_time = Keyword.get(opts, :start_time, 0)
    interval = Keyword.get(opts, :interval, 60)
    volatility = Keyword.get(opts, :volatility, 0.02)

    1..count
    |> Enum.map_reduce(base_price, fn i, prev_close ->
      time = start_time + (i - 1) * interval

      candle =
        random_candle(
          base_price: prev_close,
          time: time,
          volatility: volatility
        )

      {candle, candle.close}
    end)
    |> elem(0)
  end

  @doc """
  Generates an IndicatorsRequested event.
  """
  @spec indicators_request(keyword()) :: IndicatorsRequested.t()
  def indicators_request(opts \\ []) do
    candle_count = Keyword.get(opts, :candle_count, 50)
    periods = Keyword.get(opts, :periods, [5, 10, 20])

    %IndicatorsRequested{
      pair: Keyword.get(opts, :pair, :btc_usd),
      timeframe: Keyword.get(opts, :timeframe, :"1m"),
      periods: periods,
      candles: Keyword.get(opts, :candles, random_candles(candle_count)),
      timestamp: Keyword.get(opts, :timestamp, DateTime.utc_now()),
      version: 1
    }
  end
end
