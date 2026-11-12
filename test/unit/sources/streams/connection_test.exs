defmodule Oracle.Sources.Streams.ConnectionTest do
  use ExUnit.Case, async: true

  alias Oracle.Feeds.{Kline, Trade}
  alias Oracle.Sources.Streams.Connection

  @uri %{scheme: :wss, host: "stream.binance.com", port: 443, path: "/ws"}

  describe "connection_plan/2 with no proxy configured" do
    test "opens TLS straight to the origin and carries no tunnel" do
      plan = Connection.connection_plan(@uri, nil)

      assert plan.open_host == ~c"stream.binance.com"
      assert plan.open_port == 443
      assert plan.tunnel == nil
      assert plan.open_opts.transport == :tls
      assert plan.open_opts.protocols == [:http]
      assert plan.open_opts.tls_opts[:verify] == :verify_peer
      assert plan.open_opts.tls_opts[:server_name_indication] == ~c"stream.binance.com"
    end
  end

  describe "connection_plan/2 through an HTTP CONNECT proxy" do
    test "opens plain TCP to the proxy and carries the origin as the tunnel destination" do
      plan = Connection.connection_plan(@uri, {:http, "127.0.0.1", 9080})

      assert plan.open_host == ~c"127.0.0.1"
      assert plan.open_port == 9080
      assert plan.open_opts.transport == :tcp
      assert plan.open_opts.protocols == [:http]
      refute Map.has_key?(plan.open_opts, :tls_opts)
      assert plan.tunnel.host == ~c"stream.binance.com"
      assert plan.tunnel.port == 443
      assert plan.tunnel.transport == :tls
      assert plan.tunnel.protocols == [:http]
      assert plan.tunnel.tls_opts[:verify] == :verify_peer
      assert plan.tunnel.tls_opts[:server_name_indication] == ~c"stream.binance.com"
    end

    test "a plain ws origin tunnels without TLS" do
      uri = %{scheme: :ws, host: "localhost", port: 8080, path: "/ws"}
      plan = Connection.connection_plan(uri, {:http, "127.0.0.1", 9080})

      assert plan.tunnel.transport == :tcp
      refute Map.has_key?(plan.tunnel, :tls_opts)
    end

    test "a malformed proxy config fails loudly instead of falling back to clearnet" do
      assert_raise FunctionClauseError, fn ->
        Connection.connection_plan(@uri, {:socks5, "127.0.0.1", 9050})
      end
    end
  end

  describe "valid-trade watchdog" do
    defmodule DoomedAdapter do
      @moduledoc false
      alias Oracle.Feeds.{Kline, Trade}

      def name, do: :doomed
      def ws_url(_), do: "ws://127.0.0.1:1/ws"
      def subscribe_messages(_), do: []
      def ping_config, do: nil

      def parse_message(%{"trade" => pair}) do
        {:ok,
         [
           %Trade{
             source: :doomed,
             pair: String.to_existing_atom(pair),
             trade_id: "trade-1",
             price: Decimal.new("100"),
             quantity: Decimal.new("1"),
             side: :buy,
             timestamp: DateTime.from_unix!(1_700_000_000, :second)
           }
         ]}
      end

      def parse_message(%{"kline" => pair}) do
        {:ok,
         [
           %Kline{
             source: :doomed,
             pair: pair,
             timeframe: :"1m",
             open: Decimal.new("100"),
             high: Decimal.new("100"),
             low: Decimal.new("100"),
             close: Decimal.new("100"),
             timestamp: DateTime.from_unix!(1_700_000_000, :second)
           }
         ]}
      end

      def parse_message(_), do: :ignore
    end

    setup do
      {:ok, pid} =
        Connection.start_link(
          adapter: DoomedAdapter,
          channels: [
            %{feed: :trades, pair: :btc_usdt},
            %{feed: :klines, pair: :btc_usdt}
          ],
          target: self(),
          tag: :tick,
          receive_timeout_ms: nil
        )

      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 100) end)

      {:ok, pid: pid}
    end

    defp inject_ready_state(pid, timeout_ms, channels \\ nil) do
      ws_ref = make_ref()
      gun_pid = self()

      :sys.replace_state(pid, fn state ->
        %{
          state
          | gun_pid: gun_pid,
            ws_ref: ws_ref,
            conn_state: :ready,
            channels: channels || state.channels,
            receive_timeout_ms: timeout_ms,
            trade_timers: %{}
        }
      end)

      {gun_pid, ws_ref}
    end

    defp frame(pid, gun_pid, ws_ref, payload) do
      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, Jason.encode!(payload)}})
      :sys.get_state(pid)
    end

    defp valid_trade(pid, gun_pid, ws_ref, pair \\ "btc_usdt") do
      frame(pid, gun_pid, ws_ref, %{"trade" => pair})
    end

    test "nil timeout leaves trade timers disarmed", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, nil)
      state = valid_trade(pid, gun_pid, ws_ref)

      assert state.trade_timers == %{}
      assert state.conn_state == :ready
    end

    test "only a valid trade arms and re-arms its channel", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, 500)

      assert frame(pid, gun_pid, ws_ref, %{}).trade_timers == %{}
      state = valid_trade(pid, gun_pid, ws_ref)

      assert {{first, _token}, 1} =
               {Map.fetch!(state.trade_timers, :btc_usdt), map_size(state.trade_timers)}

      assert is_reference(first)

      kline_state = frame(pid, gun_pid, ws_ref, %{"kline" => "btc_usdt"})
      assert {^first, _token} = Map.fetch!(kline_state.trade_timers, :btc_usdt)

      malformed_state = frame(pid, gun_pid, ws_ref, %{"unexpected" => true})
      assert {^first, _token} = Map.fetch!(malformed_state.trade_timers, :btc_usdt)

      next_state = valid_trade(pid, gun_pid, ws_ref)
      assert {second, _token} = Map.fetch!(next_state.trade_timers, :btc_usdt)
      assert is_reference(second)
      refute second == first
    end

    test "each configured trade channel has an independent timer", %{pid: pid} do
      channels = [%{feed: :trades, pair: :btc_usdt}, %{feed: :trades, pair: :eth_usdt}]
      {gun_pid, ws_ref} = inject_ready_state(pid, 500, channels)

      send(pid, {:gun_upgrade, gun_pid, ws_ref, ["websocket"], []})
      initial = :sys.get_state(pid)
      assert Map.keys(initial.trade_timers) |> Enum.sort() == [:btc_usdt, :eth_usdt]
      {btc_first, _token} = Map.fetch!(initial.trade_timers, :btc_usdt)
      {eth_first, _token} = Map.fetch!(initial.trade_timers, :eth_usdt)

      next_state = valid_trade(pid, gun_pid, ws_ref)
      {btc_second, _token} = Map.fetch!(next_state.trade_timers, :btc_usdt)
      assert {^eth_first, _token} = Map.fetch!(next_state.trade_timers, :eth_usdt)
      refute btc_second == btc_first
    end

    test "klines and malformed frames do not prevent trade-silence reconnect", %{pid: pid} do
      import ExUnit.CaptureLog

      {gun_pid, ws_ref} = inject_ready_state(pid, 50)
      valid_trade(pid, gun_pid, ws_ref)

      log =
        capture_log(fn ->
          frame(pid, gun_pid, ws_ref, %{"kline" => "btc_usdt"})
          frame(pid, gun_pid, ws_ref, %{"unexpected" => true})
          Process.sleep(120)
          _ = :sys.get_state(pid)
        end)

      state = :sys.get_state(pid)
      assert state.conn_state == :disconnected
      assert state.trade_timers == %{}
      assert state.backoff_ms > 1_000
      assert log =~ "trade timeout"
      assert log =~ "DoomedAdapter"
      assert log =~ "timeout_ms=50"
    end

    test "teardown cancels every trade timer", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, 500)
      valid_trade(pid, gun_pid, ws_ref)
      assert map_size(:sys.get_state(pid).trade_timers) == 1

      send(pid, {:gun_down, gun_pid, :http, :closed, []})
      assert :sys.get_state(pid).trade_timers == %{}
    end
  end
end
