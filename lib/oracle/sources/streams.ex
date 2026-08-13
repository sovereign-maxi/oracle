defmodule Oracle.Sources.Streams do
  @moduledoc """
  Behaviour for WebSocket streaming data sources.

  Exchange adapters implement this behaviour to provide a consistent
  interface for real-time market data via WebSocket connections.
  """

  @type feed :: :ticker | :book_ticker | :trades | :book | :liquidations | :funding_rate

  @type channel :: %{feed: feed(), pair: atom()}

  @doc """
  Returns the source identifier.
  """
  @callback name() :: atom()

  @doc """
  Builds the WebSocket URL for the given channels.

  Some exchanges (e.g., Binance) encode subscriptions in the URL.
  Others use a single endpoint and subscribe via messages.
  """
  @callback ws_url([channel()]) :: String.t()

  @doc """
  Returns subscribe messages to send after connection.

  Each message should be a JSON-encodable map. Returns an empty
  list if subscriptions are encoded in the URL.
  """
  @callback subscribe_messages([channel()]) :: [map()]

  @doc """
  Returns unsubscribe messages for the given channels.
  """
  @callback unsubscribe_messages([channel()]) :: [map()]

  @doc """
  Parses an incoming WebSocket message into feed structs.

  ## Returns

  - `{:ok, [struct]}` - Successfully parsed into one or more feed structs
  - `:ping` - Message is a ping/pong acknowledgement. Informational only:
    the connection performs no reply for it (WebSocket keepalive is Gun's
    job; application heartbeats are driven by `ping_config`). Adapters
    needing reply-style heartbeats must schedule them via `ping_config`.
  - `:ignore` - Message should be ignored (e.g., subscription confirmations)
  - `{:error, term}` - Parse error
  """
  @callback parse_message(map()) ::
              {:ok, [struct()]} | :ping | :ignore | {:error, term()}

  @doc """
  Returns ping configuration for keepalive.

  Returns `{message, interval_ms}` where message is a JSON-encodable
  map to send as a ping, or `nil` if no application-level ping is needed.
  """
  @callback ping_config() :: {map(), pos_integer()} | nil

  @doc """
  Returns the list of feed types supported by this source.
  """
  @callback supported_feeds() :: [feed()]
end
