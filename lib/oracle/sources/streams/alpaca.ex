defmodule Oracle.Sources.Streams.Alpaca do
  @moduledoc """
  Alpaca Market Data WebSocket streaming adapter — SIP consolidated tape.

  Alpaca requires a two-step handshake before channel subscriptions
  land: an `{"action": "auth", "key": ..., "secret": ...}` frame first,
  then `{"action": "subscribe", ...}`. Credentials do not belong on the
  adapter — the connection runtime issues `auth_message/2` from
  application env and only calls `subscribe_messages/1` after the auth
  success frame arrives.

  ## WebSocket Endpoint

      wss://stream.data.alpaca.markets/v2/sip

  The `/v2/sip` variant carries the consolidated NBBO across all US
  equity exchanges and requires an Algo Trader Plus subscription. The
  `/v2/iex` variant is IEX-only and available on the free tier —
  swap `@ws_url_sip` for `@ws_url_iex` for a dev-tier connection.

  ## Supported Feeds

  - `:ticker` — real-time quote (bid/ask) messages emitted as `Ticker`
    with `price` set to midpoint. `volume_24h` / `change_24h` are `nil`
    on quote-only messages.
  - `:trades` — real-time trade messages emitted as `Trade`.

  Barrier detection uses midpoint from the latest quote; trade
  subscription is optional (last-trade display, volume tracking).

  ## Message Envelope

  Alpaca frames arrive as JSON arrays of one or more message objects
  keyed by a single-letter `T` (type). `parse_message/1` accepts an
  already-decoded map for consistency with the other stream adapters —
  the connection runtime iterates the array and calls `parse_message/1`
  on each element.
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.{Ticker, Trade}

  @ws_url_sip "wss://stream.data.alpaca.markets/v2/sip"
  @ws_url_iex "wss://stream.data.alpaca.markets/v2/iex"

  # --- Public API ---

  @doc "Websocket URL for the SIP consolidated feed (Algo Trader Plus)."
  @spec ws_url_sip() :: String.t()
  def ws_url_sip, do: @ws_url_sip

  @doc "Websocket URL for the IEX-only feed (free tier / dev)."
  @spec ws_url_iex() :: String.t()
  def ws_url_iex, do: @ws_url_iex

  @doc """
  Builds the auth frame the runtime must send first, before subscribing.
  Credentials are passed in — not read from env — so the adapter stays
  pure protocol.
  """
  @spec auth_message(String.t(), String.t()) :: map()
  def auth_message(key_id, secret)
      when is_binary(key_id) and is_binary(secret) do
    %{"action" => "auth", "key" => key_id, "secret" => secret}
  end

  # --- StreamSource behaviour ---

  @impl true
  def name, do: :alpaca

  @impl true
  def ws_url(_channels), do: @ws_url_sip

  @impl true
  def subscribe_messages(channels) do
    grouped = group_by_feed(channels)

    [
      Map.merge(
        %{"action" => "subscribe"},
        Map.new(grouped, fn {feed, pairs} ->
          {feed_to_alpaca_channel(feed), Enum.map(pairs, &pair_to_symbol/1)}
        end)
      )
    ]
  end

  @impl true
  def unsubscribe_messages(channels) do
    grouped = group_by_feed(channels)

    [
      Map.merge(
        %{"action" => "unsubscribe"},
        Map.new(grouped, fn {feed, pairs} ->
          {feed_to_alpaca_channel(feed), Enum.map(pairs, &pair_to_symbol/1)}
        end)
      )
    ]
  end

  @impl true
  def parse_message(%{"T" => "q"} = msg), do: parse_quote(msg)
  def parse_message(%{"T" => "t"} = msg), do: parse_trade(msg)
  def parse_message(%{"T" => "success"}), do: :ignore
  def parse_message(%{"T" => "subscription"}), do: :ignore
  def parse_message(%{"T" => "error"} = msg), do: {:error, {:alpaca_error, msg}}
  def parse_message(_), do: :ignore

  @impl true
  def ping_config, do: nil

  @impl true
  def supported_feeds, do: [:ticker, :trades]

  # --- Internal ---

  defp parse_quote(msg) do
    with {:ok, bid} <- safe_decimal(msg["bp"]),
         {:ok, ask} <- safe_decimal(msg["ap"]) do
      mid = Decimal.div(Decimal.add(bid, ask), Decimal.new(2))

      {:ok,
       [
         %Ticker{
           source: :alpaca,
           pair: symbol_to_pair(msg["S"]),
           price: mid,
           bid: bid,
           ask: ask,
           volume_24h: nil,
           change_24h: nil,
           high_24h: nil,
           low_24h: nil,
           timestamp: parse_time(msg["t"])
         }
       ]}
    else
      _ -> {:error, :invalid_quote_data}
    end
  end

  defp parse_trade(msg) do
    with {:ok, price} <- safe_decimal(msg["p"]),
         {:ok, qty} <- safe_decimal_or_zero(msg["s"]) do
      {:ok,
       [
         %Trade{
           source: :alpaca,
           pair: symbol_to_pair(msg["S"]),
           trade_id: trade_id(msg["i"]),
           price: price,
           quantity: qty,
           # Alpaca does not tag equity trades with a side.
           side: nil,
           timestamp: parse_time(msg["t"])
         }
       ]}
    else
      _ -> {:error, :invalid_trade_data}
    end
  end

  defp group_by_feed(channels) do
    Enum.group_by(channels, & &1.feed, & &1.pair)
  end

  defp feed_to_alpaca_channel(:ticker), do: "quotes"
  defp feed_to_alpaca_channel(:trades), do: "trades"

  # Pair naming convention across the stack: `:<ticker>_usd`
  # (e.g. `:mstr_usd`). Alpaca uses raw uppercase equity tickers.
  defp pair_to_symbol(pair) when is_atom(pair) do
    pair
    |> Atom.to_string()
    |> String.replace_suffix("_usd", "")
    |> String.upcase()
  end

  defp symbol_to_pair(symbol) when is_binary(symbol) do
    pair_name = String.downcase(symbol) <> "_usd"
    String.to_existing_atom(pair_name)
  rescue
    ArgumentError -> :unknown
  end

  defp symbol_to_pair(_), do: :unknown

  defp trade_id(nil), do: nil
  defp trade_id(id) when is_integer(id), do: Integer.to_string(id)
  defp trade_id(id) when is_binary(id), do: id
  defp trade_id(_), do: nil

  defp parse_time(time_str) when is_binary(time_str) do
    case DateTime.from_iso8601(time_str) do
      {:ok, dt, _} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp parse_time(_), do: DateTime.utc_now()

  defp safe_decimal(nil), do: {:error, :nil_value}
  defp safe_decimal(""), do: {:error, :empty_string}

  defp safe_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> {:error, {:invalid_decimal, value}}
    end
  end

  defp safe_decimal(value) when is_number(value), do: {:ok, Decimal.new("#{value}")}
  defp safe_decimal(_), do: {:error, :invalid_value}

  defp safe_decimal_or_zero(value) do
    case safe_decimal(value) do
      {:ok, _} = ok -> ok
      _ -> {:ok, Decimal.new(0)}
    end
  end
end
