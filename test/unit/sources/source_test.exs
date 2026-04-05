defmodule Oracle.Sources.SourceTest do
  use ExUnit.Case, async: true

  alias Oracle.Sources.Source

  describe "Source behaviour" do
    test "defines name/0 callback" do
      callbacks = Source.behaviour_info(:callbacks)
      assert {:name, 0} in callbacks
    end

    test "defines fetch_price/1 callback" do
      callbacks = Source.behaviour_info(:callbacks)
      assert {:fetch_price, 1} in callbacks
    end
  end

  # Example implementation for testing
  defmodule MockSource do
    @behaviour Oracle.Sources.Source

    @impl true
    def name, do: :mock

    @impl true
    def fetch_price(:btc_usd), do: {:ok, Decimal.new("100000")}
    def fetch_price(:eth_usd), do: {:ok, Decimal.new("3000")}
    def fetch_price(:unknown), do: {:error, :unsupported_pair}
  end

  describe "MockSource implementation" do
    test "returns source name" do
      assert MockSource.name() == :mock
    end

    test "fetches BTC price" do
      assert {:ok, price} = MockSource.fetch_price(:btc_usd)
      assert Decimal.equal?(price, Decimal.new("100000"))
    end

    test "fetches ETH price" do
      assert {:ok, price} = MockSource.fetch_price(:eth_usd)
      assert Decimal.equal?(price, Decimal.new("3000"))
    end

    test "returns error for unsupported pair" do
      assert {:error, :unsupported_pair} = MockSource.fetch_price(:unknown)
    end
  end
end
