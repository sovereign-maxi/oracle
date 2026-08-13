defmodule Oracle.Sources.KrakenTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Kraken

  # Mock HTTP client for testing
  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        # Multiple pairs in single request - check first (most specific)
        String.contains?(url, "XBTUSD") and String.contains?(url, "ETHUSD") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"error":[],"result":{"XXBTZUSD":{"c":["104523.45","0.123"]},"XETHZUSD":{"c":["3250.00","0.5"]}}})
           }}

        # Check XBTUSDT before XBTUSD since "XBTUSDT" contains "XBTUSD"
        String.contains?(url, "XBTUSDT") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"error":[],"result":{"XBTUSDT":{"c":["104500.00","0.1"],"a":["104505.00","1"],"b":["104495.00","1"],"v":["100","500"]}}})
           }}

        String.contains?(url, "XBTUSD") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"error":[],"result":{"XXBTZUSD":{"c":["104523.45","0.123"],"a":["104525.00","1"],"b":["104520.00","1"],"v":["1000","5000"]}}})
           }}

        String.contains?(url, "ETHUSD") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"error":[],"result":{"XETHZUSD":{"c":["3250.00","0.5"],"a":["3251.00","1"],"b":["3249.00","1"],"v":["500","2500"]}}})
           }}

        String.contains?(url, "INVALID") ->
          {:ok, %{status_code: 200, body: ~s({"error":["EQuery:Unknown asset pair"],"result":{}})}}

        true ->
          {:ok, %{status_code: 200, body: ~s({"error":[],"result":{}})}}
      end
    end
  end

  defmodule ErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok, %{status_code: 200, body: ~s({"error":["EQuery:Unknown asset pair"],"result":{}})}}
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :timeout}}
    end
  end

  defmodule ZeroVolumeMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s({"error":[],"result":{"XXBTZUSD":{"c":["104523.45","0.123"],"a":["104525.00","1"],"b":["104520.00","1"],"v":["0","0"]}}})
       }}
    end
  end

  setup do
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :kraken" do
      assert Kraken.name() == :kraken
    end
  end

  describe "fetch_price/1" do
    test "fetches BTC/USD price" do
      {:ok, price} = Kraken.fetch_price(:btc_usd)

      assert %Decimal{} = price
      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "fetches ETH/USD price" do
      {:ok, price} = Kraken.fetch_price(:eth_usd)

      assert Decimal.equal?(price, Decimal.new("3250.00"))
    end

    test "fetches BTC/USDT price" do
      {:ok, price} = Kraken.fetch_price(:btc_usdt)

      assert Decimal.equal?(price, Decimal.new("104500.00"))
    end

    test "returns error for invalid pair" do
      Application.put_env(:oracle, :http_client, ErrorMockHTTP)

      {:error, {:api_error, "EQuery:Unknown asset pair"}} = Kraken.fetch_price(:invalid)
    end

    test "handles connection errors" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      {:error, {:connection_error, :timeout}} = Kraken.fetch_price(:btc_usd)
    end
  end

  describe "fetch_ticker/1" do
    test "fetches detailed ticker with bid/ask" do
      {:ok, ticker} = Kraken.fetch_ticker(:btc_usd)

      assert %Decimal{} = ticker.last
      assert %Decimal{} = ticker.ask
      assert %Decimal{} = ticker.bid
      assert %Decimal{} = ticker.volume_24h

      assert Decimal.equal?(ticker.last, Decimal.new("104523.45"))
      assert Decimal.equal?(ticker.ask, Decimal.new("104525.00"))
      assert Decimal.equal?(ticker.bid, Decimal.new("104520.00"))
    end

    test "accepts zero volume on illiquid pairs" do
      Application.put_env(:oracle, :http_client, ZeroVolumeMockHTTP)

      assert {:ok, ticker} = Kraken.fetch_ticker(:btc_usd)
      assert Decimal.equal?(ticker.volume_24h, Decimal.new("0"))
    end
  end

  describe "fetch_prices/1" do
    test "fetches multiple prices" do
      {:ok, prices} = Kraken.fetch_prices([:btc_usd, :eth_usd])

      assert Map.has_key?(prices, :btc_usd)
      assert Map.has_key?(prices, :eth_usd)
    end
  end
end
