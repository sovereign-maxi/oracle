defmodule Oracle.Sources.PythTest do
  use ExUnit.Case, async: false

  alias Oracle.Sources.Pyth

  defmodule MockHTTP do
    def get(url, _headers, _opts) do
      cond do
        # Signed endpoint — MSTR
        String.contains?(url, "/v2/updates/price/latest") and
            String.contains?(
              url,
              "e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d"
            ) ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s({"binary":{"encoding":"base64","data":["VkFBLXNpZ25lZC1tc3Ry"]},) <>
                 ~s("parsed":[{"id":"e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d",) <>
                 ~s("price":{"price":"40125532100","conf":"12500000","expo":-8,"publish_time":1725900000},) <>
                 ~s("metadata":{"slot":42}}]})
           }}

        # MSTR feed id (legacy endpoint)
        String.contains?(url, "e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s([{"id":"e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d","price":{"price":"40125532100","conf":"12500000","expo":-8,"publish_time":1725900000}}])
           }}

        # BTC feed id
        String.contains?(url, "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s([{"id":"e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43","price":{"price":"6500000000000","conf":"2500000000","expo":-8,"publish_time":1725900000}}])
           }}

        # QQQ feed id — positive expo test case (unusual but supported).
        String.contains?(url, "1d24988a03a71d10e39c3d38c78bffc0e51eb6cd6d75d05ecfae6a0ab35e0c9c") ->
          {:ok,
           %{
             status_code: 200,
             body:
               ~s([{"id":"1d24988a03a71d10e39c3d38c78bffc0e51eb6cd6d75d05ecfae6a0ab35e0c9c","price":{"price":"51043","conf":"5","expo":-2,"publish_time":1725900000}}])
           }}

        true ->
          {:ok, %{status_code: 200, body: ~s([])}}
      end
    end
  end

  defmodule HttpErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok, %{status_code: 500, body: ~s({"error":"upstream"})}}
    end
  end

  defmodule ConnErrorMockHTTP do
    def get(_url, _headers, _opts) do
      {:error, %{reason: :timeout}}
    end
  end

  defmodule MalformedMockHTTP do
    def get(_url, _headers, _opts) do
      {:ok, %{status_code: 200, body: ~s({"not":"an array"})}}
    end
  end

  setup do
    Application.put_env(:oracle, :http_client, MockHTTP)
    on_exit(fn -> Application.delete_env(:oracle, :http_client) end)
    :ok
  end

  describe "name/0" do
    test "returns :pyth" do
      assert Pyth.name() == :pyth
    end
  end

  describe "fetch_price/1 — supported pairs" do
    test "fetches MSTR/USD price with negative expo scaling" do
      {:ok, price} = Pyth.fetch_price(:mstr_usd)

      # 40125532100 * 10^-8 = 401.25532100
      assert Decimal.equal?(price, Decimal.new("401.25532100"))
    end

    test "fetches BTC/USD price with large negative expo" do
      {:ok, price} = Pyth.fetch_price(:btc_usd)

      # 6500000000000 * 10^-8 = 65000.00000000
      assert Decimal.equal?(price, Decimal.new("65000.00000000"))
    end

    test "fetches QQQ/USD price" do
      {:ok, price} = Pyth.fetch_price(:qqq_usd)

      # 51043 * 10^-2 = 510.43
      assert Decimal.equal?(price, Decimal.new("510.43"))
    end
  end

  describe "fetch_price/1 — error paths" do
    test "returns :unsupported_pair for unknown pair" do
      assert {:error, {:unsupported_pair, :nonsense_pair}} =
               Pyth.fetch_price(:nonsense_pair)
    end

    test "returns :http_error on 5xx" do
      Application.put_env(:oracle, :http_client, HttpErrorMockHTTP)

      assert {:error, {:http_error, 500}} = Pyth.fetch_price(:btc_usd)
    end

    test "returns :connection_error on transport failure" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      assert {:error, {:connection_error, :timeout}} = Pyth.fetch_price(:btc_usd)
    end

    test "returns :unexpected_shape on malformed JSON body" do
      Application.put_env(:oracle, :http_client, MalformedMockHTTP)

      assert {:error, :unexpected_shape} = Pyth.fetch_price(:btc_usd)
    end
  end

  describe "fetch_price_signed/1" do
    test "returns Pyth VAA + scaled price + metadata" do
      assert {:ok, signed} = Pyth.fetch_price_signed(:mstr_usd)

      assert signed.kind == :pyth_vaa
      assert Decimal.equal?(signed.price, Decimal.new("401.25532100"))
      assert Decimal.equal?(signed.conf, Decimal.new("0.12500000"))
      assert signed.feed_id == "e1d3bdd12c3aebc47e3ac68b1c8afce3d8f9d0e29cf39c17deaf59e0f0f8b30d"
      assert signed.vaa_b64 == "VkFBLXNpZ25lZC1tc3Ry"
      assert is_binary(signed.vaa)
      assert signed.publish_time == 1_725_900_000
      assert signed.slot == 42
    end

    test "returns :unsupported_pair for unknown pair" do
      assert {:error, {:unsupported_pair, :nonsense}} =
               Pyth.fetch_price_signed(:nonsense)
    end

    test "returns :http_error on 5xx" do
      Application.put_env(:oracle, :http_client, HttpErrorMockHTTP)

      assert {:error, {:http_error, 500}} = Pyth.fetch_price_signed(:mstr_usd)
    end

    test "returns :connection_error on transport failure" do
      Application.put_env(:oracle, :http_client, ConnErrorMockHTTP)

      assert {:error, {:connection_error, :timeout}} =
               Pyth.fetch_price_signed(:mstr_usd)
    end
  end
end
