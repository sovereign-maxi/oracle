defmodule Oracle do
  @moduledoc """
  Stateless price feed primitives for event-driven applications.

  This package provides:

  - **Events** - Event structs for price data (PriceTick, PriceUpdated, etc.)
  - **Aggregator** - Price aggregation strategies (median, mean, VWAP)
  - **Derived** - Composite price calculation from underlying pairs
  - **Indicators** - Technical indicator calculations (SMA, EMA, MACD, Bollinger)
  - **Candles** - OHLC candle building from price ticks
  - **Sources.Source** - Behaviour for REST exchange adapters
  - **Sources.StreamSource** - Behaviour for WebSocket streaming adapters
  - **Feeds** - Feed data structs for streaming (Ticker, Trade, Book, Liquidation, FundingRate)
  - **Book** - Pure order book management functions
  - **Connection** - Stateless connection management utilities (backoff, health, reconnect)

  ## Event Pattern

  All handlers use a consistent pattern:

      @spec handle(input_event, context) :: {:ok, [output_event]} | {:error, reason, [output_event]}

  ## Usage

      # Aggregate prices from multiple sources
      ticks = [
        %Oracle.Events.PriceTick{source: :binance, pair: :btc_usd, price: Decimal.new("100000"), ...},
        %Oracle.Events.PriceTick{source: :coinbase, pair: :btc_usd, price: Decimal.new("100010"), ...},
        %Oracle.Events.PriceTick{source: :kraken, pair: :btc_usd, price: Decimal.new("100005"), ...}
      ]

      {:ok, [%PriceUpdated{price: median_price}]} = Oracle.Aggregator.aggregate(ticks)

      # Calculate derived prices
      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}
      {:ok, derived_events, prices} = Oracle.Derived.calculate(price_event, formulas, prices)

      # Technical indicators
      {:ok, [%IndicatorsUpdated{}]} = Oracle.Indicators.calculate(indicators_requested)

  ## Stateless Design

  This package is purely stateless. Applications instantiate their own processes
  using these building blocks and manage state (ETS, persistence) themselves.
  """
end
