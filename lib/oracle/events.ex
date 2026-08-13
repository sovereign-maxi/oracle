defmodule Oracle.Events do
  @moduledoc """
  Event structs for the Oracle package.

  ## Input Events

  - `PriceTick` - Raw price from a single source
  - `IndicatorsRequested` - Request indicator calculation

  ## Output Events

  - `PriceUpdated` - Aggregated price from multiple sources
  - `DerivedPriceUpdated` - Calculated composite price
  - `IndicatorsUpdated` - Technical indicators calculated
  - `PriceStale` - Price hasn't updated within threshold
  - `SourceFailed` - A price source failed to respond
  - `SourceOutlier` - A price source reported an outlier value

  ## Candle Events

  - `CandleClosed` - A candle period has closed
  - `CandleUpdated` - Current candle updated with new tick

  ## Streaming Events

  - `StreamConnected` - WebSocket stream connected
  - `StreamDisconnected` - WebSocket stream disconnected
  - `StreamError` - WebSocket stream error
  - `StreamHealthChanged` - Stream health status changed
  """

  # ─────────────────────────────────────────────────────────────
  # Input Events
  # ─────────────────────────────────────────────────────────────

  defmodule PriceTick do
    @moduledoc """
    Raw price from a single source.

    ## Fields

    - `source` - Exchange or data source identifier (e.g., :binance, :coinbase)
    - `pair` - Trading pair (e.g., :btc_usd, :eth_usd)
    - `price` - Price as Decimal
    - `volume` - Trading volume in base currency (optional, for VWAP)
    - `timestamp` - When the tick was received
    - `provenance` - Optional source-signed material (e.g., Pyth VAA bytes,
      Chainlink signature, publish_time from the source). Present when the
      adapter fetches a cryptographically signed feed; nil otherwise.
      Shape is source-specific — consumers pattern-match on `:kind` in the map.
    - `version` - Event schema version
    """
    @type provenance :: %{required(:kind) => atom(), optional(atom()) => term()} | nil

    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            price: Decimal.t(),
            volume: non_neg_integer() | nil,
            timestamp: DateTime.t(),
            provenance: provenance(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :price, :volume, :timestamp, :provenance, version: 1]
  end

  defmodule IndicatorsRequested do
    @moduledoc """
    Request indicator calculation.

    ## Fields

    - `pair` - Trading pair
    - `timeframe` - Candle timeframe (e.g., :"1m", :"5m", :"1h")
    - `periods` - List of periods for SMA/EMA calculation
    - `candles` - List of OHLC candle maps
    - `timestamp` - Request timestamp
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            timeframe: atom(),
            periods: [pos_integer()],
            candles: [map()],
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:pair, :timeframe, :periods, :candles, :timestamp, version: 1]
  end

  # ─────────────────────────────────────────────────────────────
  # Output Events
  # ─────────────────────────────────────────────────────────────

  defmodule PriceUpdated do
    @moduledoc """
    Aggregated price from multiple sources.

    ## Fields

    - `pair` - Trading pair
    - `price` - Aggregated price as Decimal
    - `sources` - List of sources that contributed to this price
    - `timestamp` - Latest timestamp from input ticks
    - `provenances` - Optional list of per-source provenance blobs preserved
      from the input `PriceTick`s. Same length and ordering as `sources` when
      present. `nil` on aggregations from unsigned feeds. Consumers that need
      to attest to an aggregation preserve these end-to-end so the raw signed
      data can be republished for independent verification.
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            price: Decimal.t(),
            sources: [atom()],
            timestamp: DateTime.t(),
            provenances: [PriceTick.provenance()] | nil,
            version: pos_integer()
          }

    defstruct [:pair, :price, :sources, :timestamp, :provenances, version: 1]
  end

  defmodule DerivedPriceUpdated do
    @moduledoc """
    Calculated composite price.

    ## Fields

    - `pair` - Derived trading pair (e.g., :btc_xau)
    - `price` - Calculated price as Decimal
    - `base_pair` - Base pair used in calculation
    - `quote_pair` - Quote pair used in calculation
    - `formula` - Operation used (:divide or :multiply)
    - `timestamp` - Calculation timestamp
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            price: Decimal.t(),
            base_pair: atom(),
            quote_pair: atom(),
            formula: :divide | :multiply,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:pair, :price, :base_pair, :quote_pair, :formula, :timestamp, version: 1]
  end

  defmodule IndicatorsUpdated do
    @moduledoc """
    Technical indicators calculated.

    ## Fields

    - `pair` - Trading pair
    - `timeframe` - Candle timeframe
    - `sma` - Map of period => SMA values
    - `ema` - Map of period => EMA values
    - `macd` - MACD data (macd, signal, histogram)
    - `bollinger` - Bollinger bands (upper, middle, lower)
    - `timestamp` - Calculation timestamp
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            timeframe: atom(),
            sma: map(),
            ema: map(),
            macd: map(),
            bollinger: map(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:pair, :timeframe, :sma, :ema, :macd, :bollinger, :timestamp, version: 1]
  end

  defmodule PriceStale do
    @moduledoc """
    Price hasn't updated within threshold.

    ## Fields

    - `pair` - Trading pair with stale price
    - `last_update` - DateTime of last price update
    - `threshold_seconds` - Staleness threshold in seconds
    - `timestamp` - When staleness was detected
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            last_update: DateTime.t(),
            threshold_seconds: pos_integer(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:pair, :last_update, :threshold_seconds, :timestamp, version: 1]
  end

  defmodule SourceFailed do
    @moduledoc """
    A price source failed to respond.

    ## Fields

    - `source` - Exchange or data source that failed
    - `pair` - Trading pair that failed
    - `reason` - Error reason
    - `timestamp` - When failure occurred
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            reason: term(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :reason, :timestamp, version: 1]
  end

  defmodule SourceOutlier do
    @moduledoc """
    A price source reported an outlier value.

    ## Fields

    - `source` - Exchange or data source with outlier
    - `pair` - Trading pair
    - `price` - Outlier price reported
    - `median_price` - Median price from all sources
    - `deviation_percent` - Percentage deviation from median
    - `timestamp` - When outlier was detected
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            price: Decimal.t(),
            median_price: Decimal.t(),
            deviation_percent: float(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :price, :median_price, :deviation_percent, :timestamp, version: 1]
  end

  # ─────────────────────────────────────────────────────────────
  # Candle Events
  # ─────────────────────────────────────────────────────────────

  defmodule CandleClosed do
    @moduledoc """
    A candle period has closed.

    ## Fields

    - `pair` - Trading pair
    - `timeframe` - Candle timeframe
    - `time` - Unix timestamp of candle start
    - `open` - Opening price
    - `high` - Highest price
    - `low` - Lowest price
    - `close` - Closing price
    - `volume` - Total volume
    - `timestamp` - When candle closed
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            timeframe: atom(),
            time: integer(),
            open: float(),
            high: float(),
            low: float(),
            close: float(),
            volume: non_neg_integer(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [
      :pair,
      :timeframe,
      :time,
      :open,
      :high,
      :low,
      :close,
      :volume,
      :timestamp,
      version: 1
    ]
  end

  defmodule CandleUpdated do
    @moduledoc """
    Current candle updated with new tick.

    ## Fields

    - `pair` - Trading pair
    - `timeframe` - Candle timeframe
    - `time` - Unix timestamp of candle start
    - `open` - Opening price
    - `high` - Highest price
    - `low` - Lowest price
    - `close` - Current closing price
    - `volume` - Current volume
    - `timestamp` - Update timestamp
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            pair: atom(),
            timeframe: atom(),
            time: integer(),
            open: float(),
            high: float(),
            low: float(),
            close: float(),
            volume: non_neg_integer(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [
      :pair,
      :timeframe,
      :time,
      :open,
      :high,
      :low,
      :close,
      :volume,
      :timestamp,
      version: 1
    ]
  end

  # ─────────────────────────────────────────────────────────────
  # Streaming Events
  # ─────────────────────────────────────────────────────────────

  defmodule StreamConnected do
    @moduledoc """
    WebSocket stream connected successfully.

    ## Fields

    - `source` - Exchange identifier
    - `channels` - List of subscribed channels
    - `timestamp` - When connection was established
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            channels: [map()],
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :channels, :timestamp, version: 1]
  end

  defmodule StreamDisconnected do
    @moduledoc """
    WebSocket stream disconnected.

    ## Fields

    - `source` - Exchange identifier
    - `reason` - Disconnect reason
    - `attempt` - Reconnection attempt number
    - `timestamp` - When disconnection occurred
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            reason: term(),
            attempt: non_neg_integer(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :reason, :attempt, :timestamp, version: 1]
  end

  defmodule StreamError do
    @moduledoc """
    WebSocket stream error.

    ## Fields

    - `source` - Exchange identifier
    - `reason` - Error reason
    - `raw_message` - Raw message that caused the error
    - `timestamp` - When error occurred
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            reason: term(),
            raw_message: term(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :reason, :raw_message, :timestamp, version: 1]
  end

  defmodule StreamHealthChanged do
    @moduledoc """
    Stream health status changed.

    ## Fields

    - `source` - Exchange identifier
    - `status` - New health status (:healthy, :degraded, :unhealthy)
    - `metrics` - Health metrics map
    - `timestamp` - When health changed
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            status: :healthy | :degraded | :unhealthy,
            metrics: map(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :status, :metrics, :timestamp, version: 1]
  end
end
