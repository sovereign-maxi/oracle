defmodule Oracle.Sources.CoinbaseTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Coinbase

  # Mock HTTP client for testing
  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        String.contains?(url, "BTC-USD/spot") ->
          {:ok,
           %{
             status_code: 200,
             body: ~s({"data":{"base":"BTC","currency":"USD","amount":"104523.45"}})
           }}

        String.contains?(url, "ETH-USD/spot") ->
          {:ok,
           %{
             status_code: 200,
             body: ~s({"data":{"base":"ETH","currency":"USD","amount":"3250.00"}})
           }}

        String.contains?(url, "BTC-USD/buy") ->
          {:ok,
           %{
             status_code: 200,
             body: ~s({"data":{"base":"BTC","currency":"USD","amount":"104600.00"}})
           }}

        String.contains?(url, "BTC-USD/sell") ->
          {:ok,
           %{
             status_code: 200,
             body: ~s({"data":{"base":"BTC","currency":"USD","amount":"104450.00"}})
           }}

        String.contains?(url, "INVALID") ->
          {:ok,
           %{
             status_code: 404,
             body: ~s({"errors":[{"id":"not_found","message":"Currency pair not found"}]})
           }}

        true ->
          {:ok, %{status_code: 200, body: ~s({"data":{"amount":"100.00"}})}}
      end
    end
  end

  defmodule ErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 404,
         body: ~s({"errors":[{"id":"not_found","message":"Currency pair not found"}]})
       }}
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :nxdomain}}
    end
  end

  setup do
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :coinbase" do
      assert Coinbase.name() == :coinbase
    end
  end

  describe "fetch_price/1" do
    test "fetches BTC/USD price" do
      {:ok, price} = Coinbase.fetch_price(:btc_usd)

      assert %Decimal{} = price
      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "BTC/USDT aliases to BTC/USD" do
      {:ok, price} = Coinbase.fetch_price(:btc_usdt)

      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "fetches ETH/USD price" do
      {:ok, price} = Coinbase.fetch_price(:eth_usd)

      assert Decimal.equal?(price, Decimal.new("3250.00"))
    end

    test "returns error for invalid pair" do
      # Use the main mock - it handles INVALID requests
      {:error, {:api_error, "Currency pair not found"}} = Coinbase.fetch_price(:invalid)
    end

    test "handles connection errors" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      {:error, {:connection_error, :nxdomain}} = Coinbase.fetch_price(:btc_usd)
    end
  end

  describe "fetch_prices_detailed/1" do
    test "fetches spot, buy, and sell prices" do
      {:ok, prices} = Coinbase.fetch_prices_detailed(:btc_usd)

      assert %Decimal{} = prices.spot
      assert %Decimal{} = prices.buy
      assert %Decimal{} = prices.sell

      assert Decimal.equal?(prices.spot, Decimal.new("104523.45"))
      assert Decimal.equal?(prices.buy, Decimal.new("104600.00"))
      assert Decimal.equal?(prices.sell, Decimal.new("104450.00"))
    end
  end
end
