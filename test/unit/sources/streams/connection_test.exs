defmodule Oracle.Sources.Streams.ConnectionTest do
  use ExUnit.Case, async: true

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

      # The socket gun opens points at the tunnel, not the exchange.
      assert plan.open_host == ~c"127.0.0.1"
      assert plan.open_port == 9080
      assert plan.open_opts.transport == :tcp
      assert plan.open_opts.protocols == [:http]
      refute Map.has_key?(plan.open_opts, :tls_opts)

      # The CONNECT destination is the origin, with TLS terminating
      # at the origin through the tunnel.
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

  describe "receive-side watchdog" do
    # The watchdog closes the gap between "data silent" (bounded by
    # `receive_timeout_ms`) and "TCP declared dead" (minutes, via
    # the OS retransmit timeout) — the zombie-socket class where an
    # exchange edge honors Gun's protocol pings but stops forwarding
    # stream data. The tests here drive the GenServer through
    # synthetic state injection; Gun never actually connects (the
    # adapter's URL points at a port no one listens on, so `:connect`
    # resolves fast into `:disconnected` and backoff takes over).

    defmodule DoomedAdapter do
      @moduledoc false
      def name, do: :doomed
      # Loopback + unlikely port — gun resolves + fails fast.
      def ws_url(_), do: "ws://127.0.0.1:1/ws"
      def subscribe_messages(_), do: []
      def parse_message(_), do: :ignore
      def ping_config, do: nil
    end

    setup do
      {:ok, pid} =
        Connection.start_link(
          adapter: DoomedAdapter,
          channels: [],
          target: self(),
          tag: :tick,
          receive_timeout_ms: nil
        )

      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 100) end)

      {:ok, pid: pid}
    end

    defp inject_ready_state(pid, timeout_ms) do
      # Replace whatever state the (failing) `:connect` reached
      # with a synthetic `:ready` state we can drive the handlers
      # against. `gun_pid` is set to `self()` so the matching
      # `{:gun_ws, gun_pid, ws_ref, _}` clause fires on our sends.
      ws_ref = make_ref()
      gun_pid = self()

      :sys.replace_state(pid, fn state ->
        %{
          state
          | gun_pid: gun_pid,
            ws_ref: ws_ref,
            conn_state: :ready,
            receive_timeout_ms: timeout_ms,
            receive_timer: nil
        }
      end)

      {gun_pid, ws_ref}
    end

    test "`receive_timeout_ms: nil` leaves the timer disarmed across frames", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, nil)

      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, "{}"}})
      state = :sys.get_state(pid)

      assert state.receive_timer == nil
      assert state.conn_state == :ready
    end

    test "an inbound frame arms the timer and re-arms it on each subsequent frame", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, 500)

      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, "{}"}})
      first = :sys.get_state(pid).receive_timer
      assert is_reference(first)

      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, "{}"}})
      second = :sys.get_state(pid).receive_timer

      # Fresh reference each time — proof the cancel-and-re-arm
      # path ran, not just a silent no-op.
      assert is_reference(second)
      refute second == first
    end

    test "silence past the timeout tears down + reconnects + logs the warning", %{pid: pid} do
      import ExUnit.CaptureLog

      {gun_pid, ws_ref} = inject_ready_state(pid, 50)

      # Arm the timer by delivering one frame — matches the live
      # path where `:gun_ws` is the arming event.
      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, "{}"}})
      assert is_reference(:sys.get_state(pid).receive_timer)

      log =
        capture_log(fn ->
          # Wait past the timeout; the fire path logs + schedules
          # reconnect. `:sys.get_state` barrier ensures the
          # `:receive_timeout` message has been processed.
          Process.sleep(120)
          _ = :sys.get_state(pid)
        end)

      state = :sys.get_state(pid)
      assert state.conn_state == :disconnected
      assert state.receive_timer == nil
      # Backoff advanced from the min — proof schedule_reconnect ran.
      assert state.backoff_ms > 1_000

      assert log =~ "receive timeout"
      assert log =~ "DoomedAdapter"
      assert log =~ "timeout_ms=50"
    end

    test "teardown cancels the timer so no stale `:receive_timeout` survives", %{pid: pid} do
      {gun_pid, ws_ref} = inject_ready_state(pid, 500)

      send(pid, {:gun_ws, gun_pid, ws_ref, {:text, "{}"}})
      assert is_reference(:sys.get_state(pid).receive_timer)

      # `:gun_down` reuses the shared `teardown/1`. After it fires,
      # the timer field is cleared via schedule_reconnect, and
      # the live timer is canceled inside teardown.
      send(pid, {:gun_down, gun_pid, :http, :closed, []})
      _ = :sys.get_state(pid)

      assert :sys.get_state(pid).receive_timer == nil
    end
  end
end
