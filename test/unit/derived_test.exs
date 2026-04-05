defmodule Oracle.DerivedTest do
  use ExUnit.Case, async: true

  alias Oracle.Derived
  alias Oracle.Events.PriceUpdated

  describe "calculate/3" do
    test "calculates BTC/XAU from BTC/USD and XAU/USD" do
      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}
      timestamp = DateTime.utc_now()

      # Initial BTC price - no derived yet
      btc_event = %PriceUpdated{
        pair: :btc_usd,
        price: Decimal.new("100000"),
        sources: [:binance],
        timestamp: timestamp,
        version: 1
      }

      {:ok, [], prices} = Derived.calculate(btc_event, formulas, %{})
      assert prices[:btc_usd] == Decimal.new("100000")

      # Gold price triggers derived calculation
      xau_event = %PriceUpdated{
        pair: :xau_usd,
        price: Decimal.new("2500"),
        sources: [:goldapi],
        timestamp: timestamp,
        version: 1
      }

      {:ok, [derived], updated_prices} = Derived.calculate(xau_event, formulas, prices)

      assert derived.pair == :btc_xau
      # 100000 / 2500 = 40
      assert Decimal.equal?(derived.price, Decimal.new("40"))
      assert derived.base_pair == :btc_usd
      assert derived.quote_pair == :xau_usd
      assert derived.formula == :divide
      assert updated_prices[:xau_usd] == Decimal.new("2500")
    end

    test "calculates multiple derived pairs when both update" do
      formulas = %{
        btc_xau: {:btc_usd, :xau_usd, :divide},
        btc_xag: {:btc_usd, :xag_usd, :divide}
      }

      timestamp = DateTime.utc_now()
      prices = %{xau_usd: Decimal.new("2500"), xag_usd: Decimal.new("30")}

      # BTC update should trigger both derived calculations
      btc_event = %PriceUpdated{
        pair: :btc_usd,
        price: Decimal.new("100000"),
        sources: [:binance],
        timestamp: timestamp,
        version: 1
      }

      {:ok, derived_events, _prices} = Derived.calculate(btc_event, formulas, prices)

      assert length(derived_events) == 2
      pairs = Enum.map(derived_events, & &1.pair) |> Enum.sort()
      assert pairs == [:btc_xag, :btc_xau]

      btc_xau = Enum.find(derived_events, &(&1.pair == :btc_xau))
      btc_xag = Enum.find(derived_events, &(&1.pair == :btc_xag))

      # BTC/XAU = 100000 / 2500 = 40
      assert Decimal.equal?(btc_xau.price, Decimal.new("40"))

      # BTC/XAG = 100000 / 30 = 3333.333...
      expected_xag = Decimal.div(Decimal.new("100000"), Decimal.new("30"))
      assert Decimal.equal?(btc_xag.price, expected_xag)
    end

    test "returns empty when missing required pair" do
      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}
      timestamp = DateTime.utc_now()

      # BTC updates but no XAU price yet
      btc_event = %PriceUpdated{
        pair: :btc_usd,
        price: Decimal.new("100000"),
        sources: [:binance],
        timestamp: timestamp,
        version: 1
      }

      {:ok, derived_events, prices} = Derived.calculate(btc_event, formulas, %{})

      assert derived_events == []
      assert prices[:btc_usd] == Decimal.new("100000")
    end

    test "ignores unrelated price updates" do
      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}
      timestamp = DateTime.utc_now()

      # ETH update - not in any formula
      eth_event = %PriceUpdated{
        pair: :eth_usd,
        price: Decimal.new("3000"),
        sources: [:binance],
        timestamp: timestamp,
        version: 1
      }

      {:ok, derived_events, prices} = Derived.calculate(eth_event, formulas, %{})

      assert derived_events == []
      assert prices[:eth_usd] == Decimal.new("3000")
    end

    test "handles multiply formula" do
      # Example: GOLD_BTC * BTC_USD = GOLD_USD
      formulas = %{calculated: {:factor1, :factor2, :multiply}}
      timestamp = DateTime.utc_now()

      prices = %{factor1: Decimal.new("10")}

      factor2_event = %PriceUpdated{
        pair: :factor2,
        price: Decimal.new("5"),
        sources: [:test],
        timestamp: timestamp,
        version: 1
      }

      {:ok, [derived], _prices} = Derived.calculate(factor2_event, formulas, prices)

      assert derived.pair == :calculated
      assert derived.formula == :multiply
      # 10 * 5 = 50
      assert Decimal.equal?(derived.price, Decimal.new("50"))
    end

    test "preserves timestamp from trigger event" do
      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}
      trigger_time = ~U[2026-01-15 12:30:45Z]

      prices = %{btc_usd: Decimal.new("100000")}

      xau_event = %PriceUpdated{
        pair: :xau_usd,
        price: Decimal.new("2500"),
        sources: [:goldapi],
        timestamp: trigger_time,
        version: 1
      }

      {:ok, [derived], _prices} = Derived.calculate(xau_event, formulas, prices)

      assert derived.timestamp == trigger_time
    end
  end

  describe "compute/3" do
    test "divides correctly" do
      result = Derived.compute(Decimal.new("100"), Decimal.new("50"), :divide)
      assert Decimal.equal?(result, Decimal.new("2"))
    end

    test "multiplies correctly" do
      result = Derived.compute(Decimal.new("100"), Decimal.new("2"), :multiply)
      assert Decimal.equal?(result, Decimal.new("200"))
    end

    test "handles decimal precision in division" do
      result = Derived.compute(Decimal.new("100"), Decimal.new("3"), :divide)
      # Should be 33.333...
      expected = Decimal.div(Decimal.new("100"), Decimal.new("3"))
      assert Decimal.equal?(result, expected)
    end
  end

  describe "real-world scenarios" do
    test "ETH/BTC calculation" do
      formulas = %{eth_btc: {:eth_usd, :btc_usd, :divide}}
      timestamp = DateTime.utc_now()

      prices = %{btc_usd: Decimal.new("100000")}

      eth_event = %PriceUpdated{
        pair: :eth_usd,
        price: Decimal.new("4000"),
        sources: [:binance],
        timestamp: timestamp,
        version: 1
      }

      {:ok, [derived], _prices} = Derived.calculate(eth_event, formulas, prices)

      assert derived.pair == :eth_btc
      # ETH/BTC = 4000 / 100000 = 0.04
      assert Decimal.equal?(derived.price, Decimal.new("0.04"))
    end
  end
end
