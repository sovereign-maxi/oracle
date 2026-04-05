defmodule Oracle.Sources.BinanceTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Binance

  # Mock HTTP client for testing
  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        # Check multi-symbol request first (most specific)
        String.contains?(url, "symbols=") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s([{"symbol":"BTCUSDT","price":"104523.45"},{"symbol":"ETHUSDT","price":"3250.00"}])
           }}

        String.contains?(url, "BTCUSDT") ->
          {:ok, %{status_code: 200, body: ~s({"symbol":"BTCUSDT","price":"104523.45"})}}

        String.contains?(url, "ETHUSDT") ->
          {:ok, %{status_code: 200, body: ~s({"symbol":"ETHUSDT","price":"3250.00"})}}

        String.contains?(url, "INVALID") ->
          {:ok, %{status_code: 400, body: ~s({"code":-1121,"msg":"Invalid symbol."})}}

        true ->
          {:ok, %{status_code: 200, body: ~s({"symbol":"UNKNOWN","price":"100.00"})}}
      end
    end
  end

  defmodule ErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok, %{status_code: 400, body: ~s({"code":-1121,"msg":"Invalid symbol."})}}
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :timeout}}
    end
  end

  setup do
    # Configure mock HTTP client
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :binance" do
      assert Binance.name() == :binance
    end
  end

  describe "fetch_price/1" do
    test "fetches BTC/USDT price" do
      {:ok, price} = Binance.fetch_price(:btc_usdt)

      assert %Decimal{} = price
      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "BTC/USD aliases to BTC/USDT" do
      {:ok, price} = Binance.fetch_price(:btc_usd)

      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "ETH/USD aliases to ETH/USDT" do
      {:ok, price} = Binance.fetch_price(:eth_usd)

      assert Decimal.equal?(price, Decimal.new("3250.00"))
    end

    test "returns error for API errors" do
      Application.put_env(:oracle, :http_client, ErrorMockHTTP)

      {:error, {:api_error, -1121, "Invalid symbol."}} = Binance.fetch_price(:btc_usdt)
    end

    test "handles connection errors" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      {:error, {:connection_error, :timeout}} = Binance.fetch_price(:btc_usdt)
    end
  end

  describe "fetch_prices/1" do
    test "fetches multiple prices in one request" do
      {:ok, prices} = Binance.fetch_prices([:btc_usdt, :eth_usdt])

      assert Map.has_key?(prices, :btc_usdt)
      assert Map.has_key?(prices, :eth_usdt)
      assert Decimal.equal?(prices[:btc_usdt], Decimal.new("104523.45"))
    end
  end
end
