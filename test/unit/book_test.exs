defmodule Oracle.BookTest do
  use ExUnit.Case, async: true

  alias Oracle.Book
  alias Oracle.Feeds.{BookDelta, BookSnapshot}

  defp d(val), do: Decimal.new(val)

  defp sample_snapshot do
    %BookSnapshot{
      source: :binance,
      pair: :btc_usdt,
      bids: [{d("104520"), d("2.5")}, {d("104510"), d("3.0")}, {d("104500"), d("1.0")}],
      asks: [{d("104525"), d("1.8")}, {d("104530"), d("4.2")}, {d("104540"), d("2.0")}],
      sequence: 100,
      timestamp: DateTime.utc_now()
    }
  end

  describe "new/0" do
    test "returns empty book" do
      book = Book.new()
      assert book.bids == []
      assert book.asks == []
      assert book.sequence == nil
    end
  end

  describe "from_snapshot/1" do
    test "initializes from snapshot" do
      book = Book.from_snapshot(sample_snapshot())
      assert length(book.bids) == 3
      assert length(book.asks) == 3
      assert book.sequence == 100
    end

    test "sorts bids descending" do
      book = Book.from_snapshot(sample_snapshot())
      [{p1, _}, {p2, _}, {p3, _}] = book.bids
      assert Decimal.gt?(p1, p2)
      assert Decimal.gt?(p2, p3)
    end

    test "sorts asks ascending" do
      book = Book.from_snapshot(sample_snapshot())
      [{p1, _}, {p2, _}, {p3, _}] = book.asks
      assert Decimal.lt?(p1, p2)
      assert Decimal.lt?(p2, p3)
    end
  end

  describe "apply_delta/2" do
    test "applies updates to existing levels" do
      book = Book.from_snapshot(sample_snapshot())

      delta = %BookDelta{
        source: :binance,
        pair: :btc_usdt,
        bids: [{d("104520"), d("5.0")}],
        asks: [],
        first_sequence: 101,
        last_sequence: 101,
        timestamp: DateTime.utc_now()
      }

      assert {:ok, updated} = Book.apply_delta(book, delta)
      {best_bid, _} = List.first(updated.bids)
      assert Decimal.equal?(best_bid, d("104520"))
    end

    test "removes levels with zero quantity" do
      book = Book.from_snapshot(sample_snapshot())

      delta = %BookDelta{
        source: :binance,
        pair: :btc_usdt,
        bids: [{d("104510"), d("0")}],
        asks: [],
        first_sequence: 101,
        last_sequence: 101,
        timestamp: DateTime.utc_now()
      }

      assert {:ok, updated} = Book.apply_delta(book, delta)
      assert length(updated.bids) == 2
    end

    test "adds new levels" do
      book = Book.from_snapshot(sample_snapshot())

      delta = %BookDelta{
        source: :binance,
        pair: :btc_usdt,
        bids: [{d("104515"), d("1.0")}],
        asks: [],
        first_sequence: 101,
        last_sequence: 101,
        timestamp: DateTime.utc_now()
      }

      assert {:ok, updated} = Book.apply_delta(book, delta)
      assert length(updated.bids) == 4
    end

    test "returns error on sequence gap" do
      book = Book.from_snapshot(sample_snapshot())

      delta = %BookDelta{
        source: :binance,
        pair: :btc_usdt,
        bids: [],
        asks: [],
        first_sequence: 200,
        last_sequence: 200,
        timestamp: DateTime.utc_now()
      }

      assert {:error, :sequence_gap} = Book.apply_delta(book, delta)
    end

    test "updates sequence" do
      book = Book.from_snapshot(sample_snapshot())

      delta = %BookDelta{
        source: :binance,
        pair: :btc_usdt,
        bids: [],
        asks: [],
        first_sequence: 101,
        last_sequence: 105,
        timestamp: DateTime.utc_now()
      }

      assert {:ok, updated} = Book.apply_delta(book, delta)
      assert updated.sequence == 105
    end
  end

  describe "truncate/2" do
    test "limits depth on each side" do
      book = Book.from_snapshot(sample_snapshot())
      truncated = Book.truncate(book, 2)
      assert length(truncated.bids) == 2
      assert length(truncated.asks) == 2
    end

    test "preserves sequence" do
      book = Book.from_snapshot(sample_snapshot())
      truncated = Book.truncate(book, 1)
      assert truncated.sequence == 100
    end
  end

  describe "best_bid_ask/1" do
    test "returns best bid and ask" do
      book = Book.from_snapshot(sample_snapshot())
      {bid, ask} = Book.best_bid_ask(book)
      assert Decimal.equal?(bid, d("104520"))
      assert Decimal.equal?(ask, d("104525"))
    end

    test "returns nils for empty book" do
      assert {nil, nil} = Book.best_bid_ask(Book.new())
    end
  end

  describe "mid_price/1" do
    test "returns mid price" do
      book = Book.from_snapshot(sample_snapshot())
      mid = Book.mid_price(book)
      expected = Decimal.div(Decimal.add(d("104520"), d("104525")), 2)
      assert Decimal.equal?(mid, expected)
    end

    test "returns nil for empty book" do
      assert is_nil(Book.mid_price(Book.new()))
    end
  end

  describe "spread/1" do
    test "returns absolute spread" do
      book = Book.from_snapshot(sample_snapshot())
      spread = Book.spread(book)
      assert Decimal.equal?(spread, d("5"))
    end

    test "returns nil for empty book" do
      assert is_nil(Book.spread(Book.new()))
    end
  end

  describe "spread_pct/1" do
    test "returns spread as percentage" do
      book = Book.from_snapshot(sample_snapshot())
      pct = Book.spread_pct(book)
      assert is_float(pct)
      assert pct > 0
      assert pct < 1
    end

    test "returns nil for empty book" do
      assert is_nil(Book.spread_pct(Book.new()))
    end
  end

  describe "depth_at_pct/2" do
    test "calculates cumulative depth" do
      book = Book.from_snapshot(sample_snapshot())
      depth = Book.depth_at_pct(book, 1)
      assert %{bid_depth: _, ask_depth: _} = depth
      assert Decimal.positive?(depth.bid_depth)
      assert Decimal.positive?(depth.ask_depth)
    end

    test "returns zero depth for empty book" do
      depth = Book.depth_at_pct(Book.new(), 1)
      assert Decimal.equal?(depth.bid_depth, d("0"))
      assert Decimal.equal?(depth.ask_depth, d("0"))
    end
  end

  describe "valid_sequence?/2" do
    test "returns true when book has no sequence" do
      book = Book.new()
      delta = %BookDelta{first_sequence: 100, last_sequence: 100}
      assert Book.valid_sequence?(book, delta)
    end

    test "returns true when delta has no sequence" do
      book = %{bids: [], asks: [], sequence: 100}
      delta = %BookDelta{first_sequence: nil, last_sequence: nil}
      assert Book.valid_sequence?(book, delta)
    end

    test "returns true for consecutive sequence" do
      book = %{bids: [], asks: [], sequence: 100}
      delta = %BookDelta{first_sequence: 101, last_sequence: 101}
      assert Book.valid_sequence?(book, delta)
    end

    test "returns false for sequence gap" do
      book = %{bids: [], asks: [], sequence: 100}
      delta = %BookDelta{first_sequence: 200, last_sequence: 200}
      refute Book.valid_sequence?(book, delta)
    end
  end
end
