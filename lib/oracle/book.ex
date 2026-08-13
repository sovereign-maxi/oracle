defmodule Oracle.Book do
  @moduledoc """
  Pure functions for order book management.

  Operates on a `book()` map type. Applications maintain book state
  and pass it to these functions for updates and queries.

  ## Usage

      # Initialize from snapshot
      book = Oracle.Book.from_snapshot(snapshot)

      # Apply incremental updates
      {:ok, book} = Oracle.Book.apply_delta(book, delta)

      # Query the book
      {best_bid, best_ask} = Oracle.Book.best_bid_ask(book)
      mid = Oracle.Book.mid_price(book)
      spread = Oracle.Book.spread_pct(book)
  """

  alias Oracle.Feeds.{BookDelta, BookSnapshot}

  @type book :: %{
          bids: [{Decimal.t(), Decimal.t()}],
          asks: [{Decimal.t(), Decimal.t()}],
          sequence: non_neg_integer() | nil
        }

  @doc """
  Creates a new empty book.
  """
  @spec new() :: book()
  def new do
    %{bids: [], asks: [], sequence: nil}
  end

  @doc """
  Initializes a book from a snapshot.
  """
  @spec from_snapshot(BookSnapshot.t()) :: book()
  def from_snapshot(%BookSnapshot{bids: bids, asks: asks, sequence: seq}) do
    %{
      bids: sort_bids(bids),
      asks: sort_asks(asks),
      sequence: seq
    }
  end

  @doc """
  Applies an incremental delta to the book.

  Returns `{:error, :sequence_gap}` if the delta's first_sequence
  doesn't follow the book's current sequence.

  A fully-stale delta (`last_sequence <= book.sequence`) is a no-op:
  every update in it is already reflected in the book, and applying it
  would resurrect removed levels and regress the sequence.
  """
  @spec apply_delta(book(), BookDelta.t()) :: {:ok, book()} | {:error, :sequence_gap}
  def apply_delta(book, %BookDelta{} = delta) do
    cond do
      stale_delta?(book, delta) ->
        {:ok, book}

      valid_sequence?(book, delta) ->
        updated_bids =
          book.bids
          |> apply_updates(delta.bids)
          |> sort_bids()

        updated_asks =
          book.asks
          |> apply_updates(delta.asks)
          |> sort_asks()

        {:ok,
         %{
           bids: updated_bids,
           asks: updated_asks,
           sequence: delta.last_sequence || book.sequence
         }}

      true ->
        {:error, :sequence_gap}
    end
  end

  defp stale_delta?(book, delta) do
    not is_nil(book.sequence) and
      not is_nil(delta.last_sequence) and
      delta.last_sequence <= book.sequence
  end

  @doc """
  Truncates the book to the given depth on each side.
  """
  @spec truncate(book(), pos_integer()) :: book()
  def truncate(book, depth) do
    %{
      bids: Enum.take(book.bids, depth),
      asks: Enum.take(book.asks, depth),
      sequence: book.sequence
    }
  end

  @doc """
  Returns the best bid and ask prices.
  """
  @spec best_bid_ask(book()) :: {Decimal.t() | nil, Decimal.t() | nil}
  def best_bid_ask(book) do
    best_bid =
      case book.bids do
        [{price, _} | _] -> price
        _ -> nil
      end

    best_ask =
      case book.asks do
        [{price, _} | _] -> price
        _ -> nil
      end

    {best_bid, best_ask}
  end

  @doc """
  Returns the mid price (average of best bid and ask).
  """
  @spec mid_price(book()) :: Decimal.t() | nil
  def mid_price(book) do
    case best_bid_ask(book) do
      {nil, _} -> nil
      {_, nil} -> nil
      {bid, ask} -> Decimal.div(Decimal.add(bid, ask), 2)
    end
  end

  @doc """
  Returns the absolute spread (best ask - best bid).
  """
  @spec spread(book()) :: Decimal.t() | nil
  def spread(book) do
    case best_bid_ask(book) do
      {nil, _} -> nil
      {_, nil} -> nil
      {bid, ask} -> Decimal.sub(ask, bid)
    end
  end

  @doc """
  Returns the spread as a percentage of the mid price.
  """
  @spec spread_pct(book()) :: float() | nil
  def spread_pct(book) do
    with %Decimal{} = s <- spread(book),
         %Decimal{} = mid when not is_nil(mid) <- mid_price(book),
         false <- Decimal.equal?(mid, 0) do
      s
      |> Decimal.div(mid)
      |> Decimal.mult(100)
      |> Decimal.to_float()
    else
      _ -> nil
    end
  end

  @doc """
  Calculates the cumulative depth at a given percentage from mid price.

  Returns the total bid and ask depth within `pct`% of mid price.
  """
  @spec depth_at_pct(book(), number()) :: %{bid_depth: Decimal.t(), ask_depth: Decimal.t()}
  def depth_at_pct(book, pct) do
    case mid_price(book) do
      nil ->
        %{bid_depth: Decimal.new(0), ask_depth: Decimal.new(0)}

      mid ->
        pct_decimal = Decimal.div(Decimal.new("#{pct}"), 100)
        range = Decimal.mult(mid, pct_decimal)
        bid_floor = Decimal.sub(mid, range)
        ask_ceiling = Decimal.add(mid, range)

        bid_depth =
          book.bids
          |> Enum.filter(fn {price, _} -> Decimal.gte?(price, bid_floor) end)
          |> Enum.reduce(Decimal.new(0), fn {_, qty}, acc -> Decimal.add(acc, qty) end)

        ask_depth =
          book.asks
          |> Enum.filter(fn {price, _} -> Decimal.lte?(price, ask_ceiling) end)
          |> Enum.reduce(Decimal.new(0), fn {_, qty}, acc -> Decimal.add(acc, qty) end)

        %{bid_depth: bid_depth, ask_depth: ask_depth}
    end
  end

  @doc """
  Checks if a delta's sequence follows the book's current sequence.

  Returns `true` if:
  - The book has no sequence (first delta)
  - The delta has no first_sequence
  - The delta's first_sequence is <= book.sequence + 1
  """
  @spec valid_sequence?(book(), BookDelta.t()) :: boolean()
  def valid_sequence?(book, %BookDelta{} = delta) do
    cond do
      is_nil(book.sequence) -> true
      is_nil(delta.first_sequence) -> true
      delta.first_sequence <= book.sequence + 1 -> true
      true -> false
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Private Functions
  # ─────────────────────────────────────────────────────────────

  # Bids sorted descending by price (highest first)
  defp sort_bids(levels) do
    Enum.sort(levels, fn {a, _}, {b, _} -> Decimal.gt?(a, b) end)
  end

  # Asks sorted ascending by price (lowest first)
  defp sort_asks(levels) do
    Enum.sort(levels, fn {a, _}, {b, _} -> Decimal.lt?(a, b) end)
  end

  # Apply updates: qty 0 removes the level, otherwise upsert
  defp apply_updates(levels, updates) do
    Enum.reduce(updates, levels, fn {price, qty}, acc ->
      filtered = Enum.reject(acc, fn {p, _} -> Decimal.equal?(p, price) end)

      if Decimal.equal?(qty, 0) do
        filtered
      else
        [{price, qty} | filtered]
      end
    end)
  end
end
