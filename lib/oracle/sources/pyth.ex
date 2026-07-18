defmodule Oracle.Sources.Pyth do
  @moduledoc """
  Pyth Network price source adapter.

  Fetches aggregated prices from the Pyth Foundation's public Hermes
  HTTP endpoint. Hermes serves the latest aggregated price for every
  feed on Pythnet without on-chain interaction, API key, or fee (free
  tier is 10s update cadence, sufficient for TURBOS 5-min warrants).

  ## Supported Pairs

  Pair → Pyth feed ID (from https://pyth.network/developers/price-feed-ids):

  | Pair | Pyth Feed ID (hex, no 0x) |
  |------|---------------------------|
  | `:btc_usd`  | e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43 |
  | `:eth_usd`  | ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace |
  | `:sol_usd`  | ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d |
  | `:mstr_usd` | e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d |
  | `:qqq_usd`  | 1d24988a03a71d10e39c3d38c78bffc0e51eb6cd6d75d05ecfae6a0ab35e0c9c |
  | `:spy_usd`  | 19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5 |
  | `:aapl_usd` | 49f6b65cb1de6b10eaf75e7c03ca029c306d0357e91b5311b175084a5ad55688 |
  | `:tsla_usd` | 16dad506d7db8da01c87581c87ca897a012a153557d4d578c3b9c9e1bc0632f1 |
  | `:nvda_usd` | b1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593 |
  | `:xau_usd`  | 765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2 |

  ## API Endpoint

  ```
  GET https://hermes.pyth.network/api/latest_price_feeds?ids[]=<feed_id>
  ```

  Response includes a `price` object with `price`, `expo`, `conf`, and
  `publish_time`. Actual price = `price * 10^expo`.

  ## Rate Limits

  Free tier is 10s update cadence. Poll no faster than 10 seconds
  or Hermes will 429.
  """

  @behaviour Oracle.Sources.Source

  @base_url "https://hermes.pyth.network"
  @timeout_ms 10_000

  @impl true
  def name, do: :pyth

  @impl true
  def fetch_price(pair) do
    case feed_id(pair) do
      nil -> {:error, {:unsupported_pair, pair}}
      id -> fetch_pyth_price(id)
    end
  end

  @doc """
  Fetches a price alongside the Wormhole-signed VAA that Pyth's
  Hermes endpoint returns from `/v2/updates/price/latest`.

  Returns `{:ok, map}` where the map carries `price` as a scaled
  `Decimal` (same as `fetch_price/1`) plus the raw base64-encoded VAA
  bytes, the feed id, publish time, Pythnet slot, and confidence
  interval. The provenance shape is intended to be dropped into
  `Oracle.Events.PriceTick.provenance` under `kind: :pyth_vaa` so
  downstream attestation code has the signed source data to preserve
  and republish.

  ## Returns

  ```elixir
  {:ok, %{
    kind: :pyth_vaa,
    price: Decimal.t(),
    conf: Decimal.t(),
    feed_id: binary(),   # hex, no 0x
    vaa: binary(),       # raw bytes (base64-decoded)
    vaa_b64: binary(),   # original base64 for wire republication
    publish_time: integer(),  # unix seconds
    slot: integer() | nil
  }}
  ```
  """
  @spec fetch_price_signed(atom()) :: {:ok, map()} | {:error, term()}
  def fetch_price_signed(pair) do
    case feed_id(pair) do
      nil -> {:error, {:unsupported_pair, pair}}
      id -> fetch_pyth_signed(id)
    end
  end

  # --- Private ---

  defp fetch_pyth_price(feed_id) do
    url = "#{@base_url}/api/latest_price_feeds?ids[]=#{feed_id}"

    headers = [
      {"User-Agent", "OracleBot/1.0"},
      {"Accept", "application/json"}
    ]

    case http_client().get(url, headers, recv_timeout: @timeout_ms) do
      {:ok, %{status_code: 200, body: body}} ->
        parse_pyth_response(body)

      {:ok, %{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, %{reason: reason}} ->
        {:error, {:connection_error, reason}}

      {:error, reason} ->
        {:error, {:request_failed, reason}}
    end
  end

  defp fetch_pyth_signed(feed_id) do
    url = "#{@base_url}/v2/updates/price/latest?ids[]=#{feed_id}&encoding=base64"

    headers = [
      {"User-Agent", "OracleBot/1.0"},
      {"Accept", "application/json"}
    ]

    case http_client().get(url, headers, recv_timeout: @timeout_ms) do
      {:ok, %{status_code: 200, body: body}} ->
        parse_pyth_signed_response(body, feed_id)

      {:ok, %{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, %{reason: reason}} ->
        {:error, {:connection_error, reason}}

      {:error, reason} ->
        {:error, {:request_failed, reason}}
    end
  end

  defp parse_pyth_response(body) do
    case Jason.decode(body) do
      {:ok, [%{"price" => %{"price" => price_str, "expo" => expo}} | _]} ->
        materialize_price(price_str, expo)

      {:ok, _other} ->
        {:error, :unexpected_shape}

      {:error, _} ->
        {:error, :json_decode_error}
    end
  end

  defp parse_pyth_signed_response(body, requested_feed_id) do
    with {:ok, decoded} <- Jason.decode(body),
         {:ok, vaa_b64} <- extract_vaa(decoded),
         {:ok, parsed_entry} <- extract_parsed_entry(decoded, requested_feed_id),
         {:ok, price} <- materialize_price(parsed_entry.price_str, parsed_entry.expo),
         {:ok, conf} <- materialize_price(parsed_entry.conf_str, parsed_entry.expo),
         {:ok, vaa_bytes} <- Base.decode64(vaa_b64) do
      {:ok,
       %{
         kind: :pyth_vaa,
         price: price,
         conf: conf,
         feed_id: parsed_entry.id,
         vaa: vaa_bytes,
         vaa_b64: vaa_b64,
         publish_time: parsed_entry.publish_time,
         slot: parsed_entry.slot
       }}
    else
      {:error, _} = err -> err
      :error -> {:error, :vaa_decode_failed}
    end
  end

  defp extract_vaa(%{"binary" => %{"encoding" => "base64", "data" => [b64 | _]}})
       when is_binary(b64),
       do: {:ok, b64}

  defp extract_vaa(_), do: {:error, :vaa_missing}

  # Selects the parsed entry for the requested feed. A single-`ids[]`
  # Hermes query returns exactly one entry, so a single-entry response
  # is taken as-is (no id filtering). Multi-entry responses filter by
  # requested feed id.
  defp extract_parsed_entry(%{"parsed" => [entry]}, _requested_feed_id) do
    to_parsed_entry(entry)
  end

  defp extract_parsed_entry(%{"parsed" => entries}, requested_feed_id)
       when is_list(entries) do
    case Enum.find(entries, &feed_matches?(&1, requested_feed_id)) do
      nil -> {:error, :parsed_entry_missing}
      entry -> to_parsed_entry(entry)
    end
  end

  defp extract_parsed_entry(_, _), do: {:error, :parsed_section_missing}

  defp to_parsed_entry(
         %{
           "id" => id,
           "price" => %{
             "price" => price_str,
             "expo" => expo,
             "conf" => conf_str,
             "publish_time" => pub_ts
           }
         } = entry
       ) do
    {:ok,
     %{
       id: id,
       price_str: price_str,
       conf_str: conf_str,
       expo: expo,
       publish_time: pub_ts,
       slot: get_in(entry, ["metadata", "slot"])
     }}
  end

  defp to_parsed_entry(_), do: {:error, :parsed_entry_missing}

  defp feed_matches?(%{"id" => id}, requested) when is_binary(id) do
    String.downcase(id) == String.downcase(requested)
  end

  defp feed_matches?(_, _), do: false

  defp materialize_price(price_str, expo) when is_binary(price_str) and is_integer(expo) do
    # price_str is a decimal integer; scale by 10^expo (usually negative).
    price = Decimal.new(price_str)

    scaled =
      if expo >= 0 do
        Decimal.mult(price, Decimal.new(pow10(expo)))
      else
        Decimal.div(price, Decimal.new(pow10(-expo)))
      end

    if Decimal.positive?(scaled) do
      {:ok, scaled}
    else
      {:error, {:invalid_price, scaled}}
    end
  end

  defp materialize_price(_price_str, _expo), do: {:error, :invalid_price_format}

  defp pow10(0), do: 1
  defp pow10(n) when n > 0, do: Enum.reduce(1..n, 1, fn _, acc -> acc * 10 end)

  defp feed_id(:btc_usd),
    do: "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"

  defp feed_id(:eth_usd),
    do: "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"

  defp feed_id(:sol_usd),
    do: "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"

  defp feed_id(:mstr_usd),
    do: "e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d"

  defp feed_id(:qqq_usd),
    do: "1d24988a03a71d10e39c3d38c78bffc0e51eb6cd6d75d05ecfae6a0ab35e0c9c"

  defp feed_id(:spy_usd),
    do: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"

  defp feed_id(:aapl_usd),
    do: "49f6b65cb1de6b10eaf75e7c03ca029c306d0357e91b5311b175084a5ad55688"

  defp feed_id(:tsla_usd),
    do: "16dad506d7db8da01c87581c87ca897a012a153557d4d578c3b9c9e1bc0632f1"

  defp feed_id(:nvda_usd),
    do: "b1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593"

  defp feed_id(:xau_usd),
    do: "765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2"

  defp feed_id(_), do: nil

  defp http_client do
    Application.get_env(:oracle, :http_client, HTTPoison)
  end
end
