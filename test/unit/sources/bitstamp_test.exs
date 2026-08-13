defmodule Oracle.Sources.BitstampTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Bitstamp

  # Mock HTTP client for testing
  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        String.contains?(url, "btcusd") and String.contains?(url, "ticker_hour") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"high":"105000.00","last":"104523.45","timestamp":"1707648000","bid":"104520.00","vwap":"104500.00","volume":"1500.00","low":"104000.00","ask":"104525.00","open":"104200.00"})
           }}

        String.contains?(url, "btcusd") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"high":"105000.00","last":"104523.45","timestamp":"1707648000","bid":"104520.00","vwap":"104500.00","volume":"1500.00","low":"104000.00","ask":"104525.00","open":"104200.00"})
           }}

        String.contains?(url, "ethusd") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"high":"3300.00","last":"3250.00","timestamp":"1707648000","bid":"3249.00","vwap":"3260.00","volume":"5000.00","low":"3200.00","ask":"3251.00","open":"3220.00"})
           }}

        String.contains?(url, "invalid") ->
          {:ok, %{status_code: 404, body: ~s({"status":"error","reason":"Invalid currency pair"})}}

        true ->
          {:ok, %{status_code: 200, body: ~s({"last":"100.00"})}}
      end
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :closed}}
    end
  end

  defmodule ZeroVolumeMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s({"high":"105000.00","last":"104523.45","timestamp":"1707648000","bid":"104520.00","vwap":"0","volume":"0","low":"104000.00","ask":"104525.00","open":"104200.00"})
       }}
    end
  end

  setup do
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :bitstamp" do
      assert Bitstamp.name() == :bitstamp
    end
  end

  describe "fetch_price/1" do
    test "fetches BTC/USD price" do
      {:ok, price} = Bitstamp.fetch_price(:btc_usd)

      assert %Decimal{} = price
      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "BTC/USDT aliases to BTC/USD" do
      {:ok, price} = Bitstamp.fetch_price(:btc_usdt)

      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "fetches ETH/USD price" do
      {:ok, price} = Bitstamp.fetch_price(:eth_usd)

      assert Decimal.equal?(price, Decimal.new("3250.00"))
    end

    test "returns error for invalid pair" do
      # Uses main mock which handles "invalid" in URL
      {:error, {:api_error, "Invalid currency pair"}} = Bitstamp.fetch_price(:invalid)
    end

    test "handles connection errors" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      {:error, {:connection_error, :closed}} = Bitstamp.fetch_price(:btc_usd)
    end
  end

  describe "fetch_ticker/1" do
    test "fetches detailed ticker information" do
      {:ok, ticker} = Bitstamp.fetch_ticker(:btc_usd)

      assert %Decimal{} = ticker.last
      assert %Decimal{} = ticker.high
      assert %Decimal{} = ticker.low
      assert %Decimal{} = ticker.open
      assert %Decimal{} = ticker.volume
      assert %Decimal{} = ticker.vwap
      assert %Decimal{} = ticker.bid
      assert %Decimal{} = ticker.ask
      assert %DateTime{} = ticker.timestamp

      assert Decimal.equal?(ticker.last, Decimal.new("104523.45"))
      assert Decimal.equal?(ticker.high, Decimal.new("105000.00"))
      assert Decimal.equal?(ticker.low, Decimal.new("104000.00"))
    end

    test "accepts zero volume and vwap on illiquid pairs" do
      Application.put_env(:oracle, :http_client, ZeroVolumeMockHTTP)

      assert {:ok, ticker} = Bitstamp.fetch_ticker(:btc_usd)
      assert Decimal.equal?(ticker.volume, Decimal.new("0"))
      assert Decimal.equal?(ticker.vwap, Decimal.new("0"))
    end
  end

  describe "fetch_ticker_hourly/1" do
    test "fetches hourly ticker data" do
      {:ok, ticker} = Bitstamp.fetch_ticker_hourly(:btc_usd)

      assert %Decimal{} = ticker.last
      assert %Decimal{} = ticker.high
      assert %Decimal{} = ticker.low
      assert %Decimal{} = ticker.open
      assert %Decimal{} = ticker.volume
      assert %Decimal{} = ticker.vwap
    end
  end
end
