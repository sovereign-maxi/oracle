defmodule Oracle.Derived do
  @moduledoc """
  Derived price calculation for composite markets.

  Calculates prices for synthetic pairs from underlying pairs.

  ## Examples

  - BTC/XAU = BTC/USD / XAU/USD (BTC priced in gold ounces)
  - BTC/XAG = BTC/USD / XAG/USD (BTC priced in silver ounces)
  - ETH/BTC = ETH/USD / BTC/USD (ETH priced in BTC)

  ## Usage

      formulas = %{
        btc_xau: {:btc_usd, :xau_usd, :divide},
        btc_xag: {:btc_usd, :xag_usd, :divide}
      }

      # When BTC/USD updates
      btc_event = %PriceUpdated{pair: :btc_usd, price: Decimal.new("100000"), ...}
      {:ok, [], prices} = Oracle.Derived.calculate(btc_event, formulas, %{})

      # When XAU/USD updates, we can now calculate BTC/XAU
      xau_event = %PriceUpdated{pair: :xau_usd, price: Decimal.new("2500"), ...}
      {:ok, [%DerivedPriceUpdated{pair: :btc_xau, price: 40}], prices} =
        Oracle.Derived.calculate(xau_event, formulas, prices)
  """

  alias Oracle.Events.{DerivedPriceUpdated, PriceUpdated}

  @type formula :: {atom(), atom(), :divide | :multiply}
  @type price_cache :: %{atom() => Decimal.t()}

  @doc """
  Attempts to calculate derived prices when an underlying price updates.

  ## Parameters

  - `event` - PriceUpdated event for an underlying pair
  - `formulas` - Map of derived_pair => {base, quote, operation}
  - `prices` - Current price cache for all pairs

  ## Returns

  - `{:ok, [DerivedPriceUpdated], updated_prices}`

  ## Examples

      formulas = %{btc_xau: {:btc_usd, :xau_usd, :divide}}

      # First BTC update - no derived prices yet
      btc_event = %PriceUpdated{pair: :btc_usd, price: Decimal.new("100000"), ...}
      {:ok, [], prices} = Oracle.Derived.calculate(btc_event, formulas, %{})

      # XAU update triggers derived calculation
      xau_event = %PriceUpdated{pair: :xau_usd, price: Decimal.new("2500"), ...}
      {:ok, [%DerivedPriceUpdated{pair: :btc_xau, price: price}], _prices} =
        Oracle.Derived.calculate(xau_event, formulas, prices)
      # price = 100000 / 2500 = 40
  """
  @spec calculate(PriceUpdated.t(), %{atom() => formula()}, price_cache()) ::
          {:ok, [DerivedPriceUpdated.t()], price_cache()}
  def calculate(%PriceUpdated{} = event, formulas, prices) do
    prices = Map.put(prices, event.pair, event.price)

    derived_events =
      formulas
      |> Enum.filter(fn {_derived, {base, quote, _op}} ->
        event.pair in [base, quote] and
          Map.has_key?(prices, base) and
          Map.has_key?(prices, quote)
      end)
      |> Enum.flat_map(fn {derived_pair, {base, quote, op}} ->
        case compute(prices[base], prices[quote], op) do
          {:error, _reason} ->
            []

          price ->
            [
              %DerivedPriceUpdated{
                pair: derived_pair,
                price: price,
                base_pair: base,
                quote_pair: quote,
                formula: op,
                timestamp: event.timestamp,
                version: 1
              }
            ]
        end
      end)

    {:ok, derived_events, prices}
  end

  @doc """
  Compute derived price from base and quote.

  ## Examples

      iex> Oracle.Derived.compute(Decimal.new(100), Decimal.new(50), :divide)
      Decimal.new(2)

      iex> Oracle.Derived.compute(Decimal.new(100), Decimal.new(2), :multiply)
      Decimal.new(200)
  """
  @spec compute(Decimal.t(), Decimal.t(), :divide | :multiply) ::
          Decimal.t() | {:error, :zero_quote_price}

  def compute(base, quote_price, :divide) do
    if Decimal.equal?(quote_price, Decimal.new(0)) do
      {:error, :zero_quote_price}
    else
      Decimal.div(base, quote_price)
    end
  end

  def compute(base, quote_price, :multiply), do: Decimal.mult(base, quote_price)
end
