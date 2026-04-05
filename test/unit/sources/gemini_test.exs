defmodule Oracle.Sources.GeminiTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Gemini

  # Mock HTTP client for testing
  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        String.contains?(url, "/v2/ticker/btcusd") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"symbol":"BTCUSD","open":"104200.00","high":"105000.00","low":"104000.00","close":"104523.45","bid":"104520.00","ask":"104525.00","changes":["0.31"]})
           }}

        String.contains?(url, "/v1/pubticker/btcusd") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"bid":"104520.00","ask":"104525.00","last":"104523.45","volume":{"BTC":"1500.00","USD":"156785000.00","timestamp":1707648000000}})
           }}

        String.contains?(url, "/v1/pubticker/ethusd") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"bid":"3249.00","ask":"3251.00","last":"3250.00","volume":{"ETH":"5000.00","USD":"16250000.00","timestamp":1707648000000}})
           }}

        String.contains?(url, "/v1/symbols") ->
          {:ok,
           %{
             status_code: 200,
             body: ~s(["btcusd","ethusd","ethbtc","ltcusd","ltcbtc"])
           }}

        String.contains?(url, "invalid") ->
          {:ok,
           %{
             status_code: 400,
             body: ~s({"result":"error","reason":"InvalidSymbol","message":"Invalid symbol"})
           }}

        true ->
          {:ok, %{status_code: 200, body: ~s({"last":"100.00"})}}
      end
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :econnrefused}}
    end
  end

  setup do
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :gemini" do
      assert Gemini.name() == :gemini
    end
  end

  describe "fetch_price/1" do
    test "fetches BTC/USD price" do
      {:ok, price} = Gemini.fetch_price(:btc_usd)

      assert %Decimal{} = price
      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "BTC/USDT aliases to BTC/USD" do
      {:ok, price} = Gemini.fetch_price(:btc_usdt)

      assert Decimal.equal?(price, Decimal.new("104523.45"))
    end

    test "fetches ETH/USD price" do
      {:ok, price} = Gemini.fetch_price(:eth_usd)

      assert Decimal.equal?(price, Decimal.new("3250.00"))
    end

    test "returns error for invalid pair" do
      # Uses main mock which handles "invalid" in URL
      {:error, {:api_error, "InvalidSymbol"}} = Gemini.fetch_price(:invalid)
    end

    test "handles connection errors" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      {:error, {:connection_error, :econnrefused}} = Gemini.fetch_price(:btc_usd)
    end
  end

  describe "fetch_ticker/1" do
    test "fetches detailed ticker with bid/ask and volume" do
      {:ok, ticker} = Gemini.fetch_ticker(:btc_usd)

      assert %Decimal{} = ticker.last
      assert %Decimal{} = ticker.bid
      assert %Decimal{} = ticker.ask
      assert is_map(ticker.volume)

      assert Decimal.equal?(ticker.last, Decimal.new("104523.45"))
      assert Decimal.equal?(ticker.bid, Decimal.new("104520.00"))
      assert Decimal.equal?(ticker.ask, Decimal.new("104525.00"))
    end
  end

  describe "fetch_ticker_v2/1" do
    test "fetches V2 ticker with OHLC data" do
      {:ok, ticker} = Gemini.fetch_ticker_v2(:btc_usd)

      assert %Decimal{} = ticker.open
      assert %Decimal{} = ticker.high
      assert %Decimal{} = ticker.low
      assert %Decimal{} = ticker.close
      assert %Decimal{} = ticker.bid
      assert %Decimal{} = ticker.ask

      assert Decimal.equal?(ticker.close, Decimal.new("104523.45"))
      assert Decimal.equal?(ticker.high, Decimal.new("105000.00"))
      assert Decimal.equal?(ticker.low, Decimal.new("104000.00"))
    end
  end

  describe "list_symbols/0" do
    test "returns list of available symbols" do
      {:ok, symbols} = Gemini.list_symbols()

      assert is_list(symbols)
      assert "btcusd" in symbols
      assert "ethusd" in symbols
    end
  end
end
