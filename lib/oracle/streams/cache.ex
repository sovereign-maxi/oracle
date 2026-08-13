defmodule Oracle.Streams.Cache do
  @moduledoc """
  In-memory last-tick cache for WebSocket streaming sources.

  A runner GenServer that owns a Mint.WebSocket connection to a
  streaming vendor writes each parsed `%Oracle.Feeds.Ticker{}` here.
  The REST-shaped `Oracle.Sources.*` façades read from here instead
  of hitting the vendor's HTTP endpoint — turning "poll" into "read
  whatever the push-feed last delivered".

  Keyed on `{source, pair}`. Value is `{%Ticker{}, written_at_ms}`.

  This module intentionally exposes no config or startup surface —
  the ETS table is lazily created on first write / read. Any process
  in the same node can write; the readers are all in-process (no
  cross-node semantics needed — the streaming subscription is
  per-node).

  The table is owned by whichever process creates it first. Call
  `ensure_table/0` once from a long-lived process at application boot
  so the table outlives individual connection processes.
  """

  alias Oracle.Feeds.Ticker

  @table __MODULE__

  @doc """
  Records the latest tick for `{source, pair}`. Idempotent — no
  ordering guarantee vs concurrent writes for the same key beyond
  ETS's own atomicity.
  """
  @spec put(atom(), atom(), Ticker.t()) :: :ok
  def put(source, pair, %Ticker{} = tick) when is_atom(source) and is_atom(pair) do
    ensure_table()
    :ets.insert(@table, {{source, pair}, tick, System.system_time(:millisecond)})
    :ok
  end

  @doc """
  Reads the most recent tick for `{source, pair}`. Returns
  `{:error, :stale}` if the tick is older than `max_age_ms`
  (defaults to 5 seconds — a WebSocket that hasn't emitted in 5s
  is effectively broken for real-time price purposes).
  """
  @spec latest(atom(), atom(), keyword()) ::
          {:ok, Ticker.t()} | {:error, :not_found | :stale}
  def latest(source, pair, opts \\ []) when is_atom(source) and is_atom(pair) do
    max_age_ms = Keyword.get(opts, :max_age_ms, 5_000)

    case :ets.whereis(@table) do
      :undefined -> {:error, :not_found}
      _ -> lookup_fresh(source, pair, max_age_ms)
    end
  end

  defp lookup_fresh(source, pair, max_age_ms) do
    case :ets.lookup(@table, {source, pair}) do
      [{_key, %Ticker{} = tick, written_at}] ->
        if System.system_time(:millisecond) - written_at > max_age_ms do
          {:error, :stale}
        else
          {:ok, tick}
        end

      [] ->
        {:error, :not_found}
    end
  end

  @doc false
  def ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        # Race-tolerant creation: a concurrent first writer may win.
        try do
          :ets.new(@table, [:set, :public, :named_table, {:read_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end

      _ ->
        :ok
    end
  end
end
