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

  defmodule StatsMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s({"symbol":"BTCUSDT","lastPrice":"104523.45","priceChange":"-1200.50","priceChangePercent":"-1.135","openPrice":"105723.95","highPrice":"106000.00","lowPrice":"102000.00","volume":"28432.521"})
       }}
    end
  end

  defmodule BadStatsMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s({"symbol":"BTCUSDT","lastPrice":"","priceChange":"-1.0","priceChangePercent":"-1.0","openPrice":"100.0","highPrice":"100.0","lowPrice":"99.0","volume":"10.0"})
       }}
    end
  end

  defmodule KlinesMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s([[1704067200000,"100.0","101.0","99.5","100.5","12.34",1704067259999,"0",10,"0","0","0"]])
       }}
    end
  end

  defmodule BadKlinesMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body:
           ~s([[1704067200000,"","101.0","99.5","100.5","12.34",1704067259999,"0",10,"0","0","0"]])
       }}
    end
  end

  defmodule MultiSymbolMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok,
       %{
         status_code: 200,
         body: ~s([{"symbol":"PAXGUSDT","price":"2450.10"},{"symbol":"MSTRBUSDT","price":"95.44"}])
       }}
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

    test "maps tokenized-stock and gold symbols back to pairs" do
      Application.put_env(:oracle, :http_client, MultiSymbolMockHTTP)

      assert {:ok, prices} = Binance.fetch_prices([:xau_usd, :mstrbusdt])
      assert Decimal.equal?(prices[:xau_usd], Decimal.new("2450.10"))
      assert Decimal.equal?(prices[:mstrbusdt], Decimal.new("95.44"))
    end
  end

  describe "fetch_24h_stats/1" do
    test "parses stats with full-precision fractional volume" do
      Application.put_env(:oracle, :http_client, StatsMockHTTP)

      assert {:ok, stats} = Binance.fetch_24h_stats(:btc_usdt)
      assert Decimal.equal?(stats.price, Decimal.new("104523.45"))
      assert Decimal.equal?(stats.change, Decimal.new("-1200.50"))
      assert Decimal.equal?(stats.volume_24h, Decimal.new("28432.521"))
    end

    test "malformed fields fail the call instead of emitting zeros" do
      Application.put_env(:oracle, :http_client, BadStatsMockHTTP)

      assert {:error, :invalid_stats_data} = Binance.fetch_24h_stats(:btc_usdt)
    end
  end

  describe "fetch_klines/2" do
    test "parses klines with full-precision fractional volume" do
      Application.put_env(:oracle, :http_client, KlinesMockHTTP)

      assert {:ok, [kline]} = Binance.fetch_klines(:btc_usdt)
      assert Decimal.equal?(kline.close, Decimal.new("100.5"))
      assert Decimal.equal?(kline.volume, Decimal.new("12.34"))
    end

    test "malformed klines fail the call instead of emitting zeros" do
      Application.put_env(:oracle, :http_client, BadKlinesMockHTTP)

      assert {:error, :invalid_kline_data} = Binance.fetch_klines(:btc_usdt)
    end

    test "empty range short-circuits to []" do
      # start_ms past end_ms — never hits the network.
      assert {:ok, []} =
               Binance.fetch_klines(:btc_usdt,
                 interval: "1m",
                 start_ms: 2_000_000,
                 end_ms: 1_000_000
               )
    end

    test "unsupported interval rejects the range call" do
      assert {:error, {:unsupported_interval, "7s"}} =
               Binance.fetch_klines(:btc_usdt,
                 interval: "7s",
                 start_ms: 1_000,
                 end_ms: 2_000
               )
    end

    test "range mode issues a startTime/endTime paged request" do
      # Record every URL the mock sees so we can assert on the shape.
      test_pid = self()

      defmodule RangeMockHTTP do
        def get(url, _headers, _opts) do
          send(:range_mock_target, {:url, url})

          {:ok,
           %{
             status_code: 200,
             body:
               ~s([[1704067200000,"100.0","101.0","99.5","100.5","12.34",1704067259999,"0",10,"0","0","0"]])
           }}
        end
      end

      Process.register(test_pid, :range_mock_target)
      Application.put_env(:oracle, :http_client, RangeMockHTTP)

      assert {:ok, [kline]} =
               Binance.fetch_klines(:btc_usdt,
                 interval: "1m",
                 start_ms: 1_704_067_200_000,
                 end_ms: 1_704_067_200_000 + 5 * 60_000
               )

      assert Decimal.equal?(kline.close, Decimal.new("100.5"))

      assert_receive {:url, url}
      assert url =~ "startTime=1704067200000"
      assert url =~ "endTime="

      Process.unregister(:range_mock_target)
    end
  end
end
