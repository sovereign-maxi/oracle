defmodule Oracle.Sources.Streams.TwelveData do
  @moduledoc """
  Twelve Data WebSocket streaming adapter.

  Push-based real-time price ticks for US equities.

  ## WebSocket Endpoint

      wss://ws.twelvedata.com/v1/quotes/price?apikey=<KEY>

  ## Subscribe / unsubscribe

      {"action":"subscribe","params":{"symbols":"MSTR,AAPL"}}
      {"action":"unsubscribe","params":{"symbols":"AAPL"}}

  ## Tick payload

      {
        "event":     "price",
        "symbol":    "MSTR",
        "currency":  "USD",
        "exchange":  "NASDAQ",
        "type":      "Common Stock",
        "timestamp": 1737494400,
        "price":     "329.20",
        "bid":       "329.19",
        "ask":       "329.21",
        "day_volume":"12345678"
      }
  """

  @behaviour Oracle.Sources.Streams

  alias Oracle.Feeds.Ticker

  @impl true
  def name, do: :twelve_data

  @impl true
  def ws_url(_channels) do
    key = api_key!()
    "wss://ws.twelvedata.com/v1/quotes/price?apikey=#{URI.encode(key)}"
  end

  @impl true
  def subscribe_messages(channels) do
    case symbols_for(channels) do
      [] -> []
      syms -> [%{"action" => "subscribe", "params" => %{"symbols" => Enum.join(syms, ",")}}]
    end
  end

  @impl true
  def unsubscribe_messages(channels) do
    case symbols_for(channels) do
      [] -> []
      syms -> [%{"action" => "unsubscribe", "params" => %{"symbols" => Enum.join(syms, ",")}}]
    end
  end

  @impl true
  def parse_message(%{"event" => "price"} = msg) do
    with {:ok, price} <- safe_decimal(msg["price"]),
         pair when pair != :unknown <- symbol_to_pair(msg["symbol"]) do
      {:ok,
       [
         %Ticker{
           source: :twelve_data,
           pair: pair,
           price: price,
           bid: opt_decimal(msg["bid"]),
           ask: opt_decimal(msg["ask"]),
           volume_24h: opt_decimal(msg["day_volume"]),
           change_24h: nil,
           high_24h: nil,
           low_24h: nil,
           timestamp: parse_time(msg["timestamp"])
         }
       ]}
    else
      _ -> {:error, :invalid_ticker_data}
    end
  end

  # Twelve Data emits {"event":"subscribe-status","status":"ok","messages":[...]}
  # after a subscribe. Nothing to feed downstream.
  def parse_message(%{"event" => "subscribe-status"}), do: :ignore
  def parse_message(%{"event" => "heartbeat"}), do: :ignore
  # Server-side errors come as {"status":"error", "code":..., "message":...}
  # without an "event" key. Surface them — a dead channel must not look
  # like a quiet one.
  def parse_message(%{"status" => "error"} = msg), do: {:error, {:twelve_data_error, msg}}
  def parse_message(_), do: :ignore

  @impl true
  # Twelve Data documents client-side heartbeat via
  #   {"action":"heartbeat"}
  # but the server sends its own periodic pings and closes idle
  # sockets after ~5 min. A 30s app-level heartbeat keeps the
  # connection warm through any middlebox with a shorter idle
  # timeout without spamming the vendor.
  def ping_config, do: {%{"action" => "heartbeat"}, 30_000}

  @impl true
  def supported_feeds, do: [:ticker]

  # --- Internal ---

  defp symbols_for(channels) do
    channels
    |> Enum.filter(&(&1.feed == :ticker))
    |> Enum.map(& &1.pair)
    |> Enum.map(&pair_to_symbol/1)
    |> Enum.reject(&(&1 == nil))
    |> Enum.uniq()
  end

  defp pair_to_symbol(pair) when is_atom(pair) do
    pair
    |> Atom.to_string()
    |> String.replace_suffix("_usd", "")
    |> String.upcase()
    |> case do
      "" -> nil
      s -> s
    end
  end

  defp pair_to_symbol(_), do: nil

  defp symbol_to_pair(symbol) when is_binary(symbol) do
    pair_name = String.downcase(symbol) <> "_usd"

    try do
      String.to_existing_atom(pair_name)
    rescue
      ArgumentError -> :unknown
    end
  end

  defp symbol_to_pair(_), do: :unknown

  defp api_key! do
    case Application.get_env(:oracle, :twelve_data, [])[:api_key] do
      key when is_binary(key) and key != "" -> key
      _ -> raise "TWELVEDATA_API_KEY not configured"
    end
  end

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

  defp opt_decimal(v) do
    case safe_decimal(v) do
      {:ok, d} -> d
      _ -> nil
    end
  end

  defp parse_time(ts) when is_integer(ts), do: DateTime.from_unix!(ts)
  defp parse_time(_), do: DateTime.utc_now()
end
