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
end
