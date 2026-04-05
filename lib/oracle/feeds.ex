defmodule Oracle.Feeds do
  @moduledoc """
  Feed data structs for WebSocket streaming.

  These structs represent real-time market data received from exchange
  WebSocket feeds. Each struct is tagged with a source and timestamp.

  ## Feed Types

  - `Ticker` - Real-time ticker/summary data
  - `Trade` - Individual trade executions
  - `BookSnapshot` - Full order book snapshot
  - `BookDelta` - Incremental order book update
  - `Liquidation` - Forced liquidation events
  - `FundingRate` - Perpetual futures funding rate
  """

  # ─────────────────────────────────────────────────────────────
  # Ticker
  # ─────────────────────────────────────────────────────────────

  defmodule Ticker do
    @moduledoc """
    Real-time ticker data from a streaming source.

    ## Fields

    - `source` - Exchange identifier (e.g., :binance, :bybit)
    - `pair` - Trading pair (e.g., :btc_usdt)
    - `price` - Last traded price
    - `bid` - Best bid price
    - `ask` - Best ask price
    - `volume_24h` - 24-hour trading volume
    - `change_24h` - 24-hour price change percentage
    - `high_24h` - 24-hour high price
    - `low_24h` - 24-hour low price
    - `timestamp` - When this data was received
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            price: Decimal.t() | nil,
            bid: Decimal.t() | nil,
            ask: Decimal.t() | nil,
            volume_24h: Decimal.t() | nil,
            change_24h: Decimal.t() | nil,
            high_24h: Decimal.t() | nil,
            low_24h: Decimal.t() | nil,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [
      :source,
      :pair,
      :price,
      :bid,
      :ask,
      :volume_24h,
      :change_24h,
      :high_24h,
      :low_24h,
      :timestamp,
      version: 1
    ]
  end

  # ─────────────────────────────────────────────────────────────
  # Trade
  # ─────────────────────────────────────────────────────────────

  defmodule Trade do
    @moduledoc """
    Individual trade execution from a streaming source.

    ## Fields

    - `source` - Exchange identifier
    - `pair` - Trading pair
    - `trade_id` - Exchange-specific trade identifier
    - `price` - Trade execution price
    - `quantity` - Trade quantity in base currency
    - `side` - Trade side (:buy or :sell)
    - `timestamp` - Trade execution time
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            trade_id: String.t() | nil,
            price: Decimal.t(),
            quantity: Decimal.t(),
            side: :buy | :sell,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :trade_id, :price, :quantity, :side, :timestamp, version: 1]
  end

  # ─────────────────────────────────────────────────────────────
  # BookSnapshot
  # ─────────────────────────────────────────────────────────────

  defmodule BookSnapshot do
    @moduledoc """
    Full order book snapshot from a streaming source.

    ## Fields

    - `source` - Exchange identifier
    - `pair` - Trading pair
    - `bids` - List of `{price, quantity}` tuples, best bid first
    - `asks` - List of `{price, quantity}` tuples, best ask first
    - `sequence` - Exchange sequence number for ordering
    - `timestamp` - When snapshot was received
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            bids: [{Decimal.t(), Decimal.t()}],
            asks: [{Decimal.t(), Decimal.t()}],
            sequence: non_neg_integer() | nil,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :sequence, :timestamp, bids: [], asks: [], version: 1]
  end

  # ─────────────────────────────────────────────────────────────
  # BookDelta
  # ─────────────────────────────────────────────────────────────

  defmodule BookDelta do
    @moduledoc """
    Incremental order book update from a streaming source.

    ## Fields

    - `source` - Exchange identifier
    - `pair` - Trading pair
    - `bids` - List of `{price, quantity}` bid updates (qty 0 = remove)
    - `asks` - List of `{price, quantity}` ask updates (qty 0 = remove)
    - `first_sequence` - First sequence number in this delta
    - `last_sequence` - Last sequence number in this delta
    - `timestamp` - When delta was received
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            bids: [{Decimal.t(), Decimal.t()}],
            asks: [{Decimal.t(), Decimal.t()}],
            first_sequence: non_neg_integer() | nil,
            last_sequence: non_neg_integer() | nil,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [
      :source,
      :pair,
      :first_sequence,
      :last_sequence,
      :timestamp,
      bids: [],
      asks: [],
      version: 1
    ]
  end

  # ─────────────────────────────────────────────────────────────
  # Liquidation
  # ─────────────────────────────────────────────────────────────

  defmodule Liquidation do
    @moduledoc """
    Forced liquidation event from a streaming source.

    ## Fields

    - `source` - Exchange identifier
    - `pair` - Trading pair
    - `side` - Position side that was liquidated (:buy or :sell)
    - `price` - Liquidation price
    - `quantity` - Liquidated quantity
    - `timestamp` - When liquidation occurred
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            side: :buy | :sell,
            price: Decimal.t(),
            quantity: Decimal.t(),
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :side, :price, :quantity, :timestamp, version: 1]
  end

  # ─────────────────────────────────────────────────────────────
  # FundingRate
  # ─────────────────────────────────────────────────────────────

  defmodule FundingRate do
    @moduledoc """
    Perpetual futures funding rate from a streaming source.

    ## Fields

    - `source` - Exchange identifier
    - `pair` - Trading pair
    - `rate` - Current funding rate as Decimal
    - `next_funding_time` - DateTime of next funding period
    - `timestamp` - When this rate was received
    - `version` - Event schema version
    """
    @type t :: %__MODULE__{
            source: atom(),
            pair: atom(),
            rate: Decimal.t(),
            next_funding_time: DateTime.t() | nil,
            timestamp: DateTime.t(),
            version: pos_integer()
          }

    defstruct [:source, :pair, :rate, :next_funding_time, :timestamp, version: 1]
  end
end
