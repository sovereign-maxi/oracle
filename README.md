# Oracle

Stateless price feed primitives: fetching, aggregation, derived price calculation, and technical indicators.

Source: [github.com/sovereign-maxi/oracle](https://github.com/sovereign-maxi/oracle)

## Installation

Add `oracle` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:oracle, path: "../oracle"}
  ]
end
```

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                           oracle                                │
│                                                                 │
│  Events In                              Events Out              │
│  ──────────                             ──────────              │
│  PriceTick ─────►┌──────────────┐─────► PriceUpdated            │
│                  │  Aggregator  │─────► SourceOutlier           │
│                  └──────────────┘                               │
│                                                                 │
│  PriceUpdated ──►┌──────────────┐─────► DerivedPriceUpdated     │
│                  │   Derived    │                               │
│                  └──────────────┘                               │
│                                                                 │
│  IndicatorsReq ─►┌──────────────┐─────► IndicatorsUpdated       │
│                  │  Indicators  │                               │
│                  └──────────────┘                               │
│                                                                 │
│  (Stateless - app manages ETS/persistence)                      │
└─────────────────────────────────────────────────────────────────┘
```

## Modules

### Core

| Module | Purpose |
|--------|---------|
| `Oracle.Aggregator` | Price aggregation (median, mean, VWAP) |
| `Oracle.Derived` | Composite price calculation (BTC/XAU, etc.) |
| `Oracle.Indicators` | Technical indicators (SMA, EMA, MACD, Bollinger) |
| `Oracle.Candles` | OHLC candle building from ticks |
| `Oracle.Book` | Pure order book management functions |
| `Oracle.Connection` | Stateless WebSocket connection utilities (backoff, health) |
| `Oracle.Feeds` | Feed data structs for streaming (Ticker, Trade, Book, Liquidation, FundingRate) |

### Behaviours

| Module | Purpose |
|--------|---------|
| `Oracle.Sources.Source` | REST exchange adapter contract |
| `Oracle.Sources.StreamSource` | WebSocket streaming adapter contract |

### REST Adapters

| Module | Purpose |
|--------|---------|
| `Oracle.Sources.Binance` | Binance API adapter |
| `Oracle.Sources.Bitstamp` | Bitstamp API adapter |
| `Oracle.Sources.Coinbase` | Coinbase API adapter |
| `Oracle.Sources.Gemini` | Gemini API adapter |
| `Oracle.Sources.Kraken` | Kraken API adapter |
| `Oracle.Sources.KuCoin` | KuCoin API adapter |
| `Oracle.Sources.Pyth` | Pyth Hermes adapter (VAA-signed prices) |
| `Oracle.Sources.TwelveData` | Twelve Data adapter (US equities, FX, indices) |
| `Oracle.Sources.Yahoo` | Yahoo Finance adapter (commodities, indices, forex) |

### Stream Adapters

| Module | Purpose |
|--------|---------|
| `Oracle.Sources.Streams.Binance` | Binance WebSocket adapter |
| `Oracle.Sources.Streams.Bybit` | Bybit WebSocket adapter |
| `Oracle.Sources.Streams.Coinbase` | Coinbase WebSocket adapter |
| `Oracle.Sources.Streams.Deribit` | Deribit WebSocket adapter |
| `Oracle.Sources.Streams.Kraken` | Kraken WebSocket adapter |
| `Oracle.Sources.Streams.Okx` | OKX WebSocket adapter |
| `Oracle.Sources.Streams.TwelveData` | Twelve Data WebSocket adapter |

## Usage

### Fetching Prices

```elixir
alias Oracle.Sources.{Binance, Coinbase, Kraken}

# Fetch from individual sources
{:ok, price} = Binance.fetch_price(:btc_usd)
{:ok, price} = Coinbase.fetch_price(:btc_usd)
{:ok, price} = Kraken.fetch_price(:btc_usd)

# Fetch multiple prices
{:ok, prices} = Binance.fetch_prices([:btc_usdt, :eth_usdt])
```

### Price Aggregation

```elixir
alias Oracle.Aggregator
alias Oracle.Events.PriceTick

# Create ticks from multiple sources
ticks = [
  %PriceTick{source: :binance, pair: :btc_usd, price: Decimal.new("104500"), timestamp: now},
  %PriceTick{source: :coinbase, pair: :btc_usd, price: Decimal.new("104520"), timestamp: now},
  %PriceTick{source: :kraken, pair: :btc_usd, price: Decimal.new("104510"), timestamp: now}
]

# Aggregate with median (default)
{:ok, [%PriceUpdated{price: price}]} = Aggregator.aggregate(ticks)

# Aggregate with VWAP
config = %{strategy: :vwap, min_sources: 2}
{:ok, events} = Aggregator.aggregate(ticks_with_volume, config)

# Detect outliers
config = %{strategy: :median, min_sources: 2, detect_outliers: true, outlier_threshold: 5.0}
{:ok, [%PriceUpdated{}, %SourceOutlier{} | _]} = Aggregator.aggregate(ticks, config)
```

### Derived Prices

Calculate composite prices from underlying pairs:

```elixir
alias Oracle.Derived
alias Oracle.Events.PriceUpdated

# Define formulas: derived_pair => {base, quote, operation}
formulas = %{
  btc_xau: {:btc_usd, :xau_usd, :divide},  # BTC priced in gold
  btc_xag: {:btc_usd, :xag_usd, :divide}   # BTC priced in silver
}

# Process price updates
prices = %{}
btc_event = %PriceUpdated{pair: :btc_usd, price: Decimal.new("100000"), ...}
{:ok, [], prices} = Derived.calculate(btc_event, formulas, prices)

xau_event = %PriceUpdated{pair: :xau_usd, price: Decimal.new("2500"), ...}
{:ok, [%DerivedPriceUpdated{pair: :btc_xau, price: price}], _} =
  Derived.calculate(xau_event, formulas, prices)
# price = 40 (BTC = 40 oz of gold)
```

### Technical Indicators

```elixir
alias Oracle.Indicators
alias Oracle.Events.IndicatorsRequested

# Request indicator calculation
request = %IndicatorsRequested{
  pair: :btc_usd,
  timeframe: :"1h",
  periods: [5, 10, 20],
  candles: candle_data,
  timestamp: DateTime.utc_now()
}

{:ok, [%IndicatorsUpdated{sma: sma, ema: ema, macd: macd, bollinger: bb}]} =
  Indicators.calculate(request)

# Direct indicator calculations
closes = [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, ...]
sma_values = Indicators.sma(closes, 5)
ema_values = Indicators.ema(closes, 5)
%{macd: macd, signal: signal, histogram: hist} = Indicators.macd(closes)
%{upper: upper, middle: middle, lower: lower} = Indicators.bollinger(closes, 20, 2)
```

### OHLC Candles

```elixir
alias Oracle.Candles

# Process ticks into candles
{:ok, events, candle} = Candles.process_tick(:btc_usd, :"1m", nil, 104500.0, timestamp)
# events = [%CandleUpdated{...}]

# Continue updating
{:ok, events, candle} = Candles.process_tick(:btc_usd, :"1m", candle, 104550.0, timestamp + 10)

# When period ends
{:ok, [%CandleClosed{}, %CandleUpdated{}], new_candle} =
  Candles.process_tick(:btc_usd, :"1m", candle, 104600.0, next_minute_timestamp)

# Aggregate timeframes
hourly_candles = Candles.aggregate_timeframe(minute_candles, :"1h")
```

## Events

### Input Events

| Event | Description |
|-------|-------------|
| `PriceTick` | Raw price from a single source |
| `IndicatorsRequested` | Request indicator calculation |

### Output Events

| Event | Description |
|-------|-------------|
| `PriceUpdated` | Aggregated price from multiple sources |
| `DerivedPriceUpdated` | Calculated composite price |
| `IndicatorsUpdated` | Technical indicators calculated |
| `PriceStale` | Price hasn't updated within threshold |
| `SourceFailed` | A price source failed to respond |
| `SourceOutlier` | A source reported an outlier value |
| `CandleClosed` | A candle period has closed |
| `CandleUpdated` | Current candle updated with new tick |

## Architecture

Oracle is **stateless** - apps manage their own:

- ETS tables for price caching
- Candle state per pair/timeframe
- Price history for derived calculations
- Source failure tracking

Example app integration:

```elixir
defmodule MyApp.PriceOracle do
  use GenServer

  def handle_info(:fetch, state) do
    # Fetch from all sources in parallel
    ticks = fetch_all_sources(state.sources, state.pairs)

    # Aggregate
    case Oracle.Aggregator.aggregate(ticks) do
      {:ok, [%PriceUpdated{} = event | _]} ->
        # Store in app's ETS
        :ets.insert(:prices, {event.pair, event.price, event.timestamp})

        # Calculate derived prices
        {:ok, derived, prices} = Oracle.Derived.calculate(event, state.formulas, state.prices)

        # Broadcast
        Phoenix.PubSub.broadcast(MyApp.PubSub, "prices", event)

        {:noreply, %{state | prices: prices}}

      {:error, :insufficient_sources, _} ->
        {:noreply, state}
    end
  end
end
```

## Development

### Pre-commit Hook

```bash
# Enable pre-commit hooks (format, credo, tests, dialyzer)
git config core.hooksPath hooks
```

### Testing

```bash
# Run tests
mix test

# Run with coverage
mix coveralls

# Check code style
mix credo --strict
```

## Dependencies

```elixir
defp deps do
  [
    {:core, path: "../core"},
    {:decimal, "~> 3.0"},
    {:jason, "~> 1.4"}
  ]
end
```

## License

MIT. See [LICENSE](LICENSE).
