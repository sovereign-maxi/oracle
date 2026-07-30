defmodule Oracle.Sources.Streams.Connection do
  @moduledoc """
  Persistent WebSocket connection GenServer for streaming price sources.

  Backed by `:gun` (Erlang HTTP/WebSocket client) — Gun owns TCP/TLS,
  ping/pong, framing, and delivers everything as asynchronous messages.
  This module is a state machine on top: connect → upgrade → subscribe
  → ready, with exponential backoff on any failure.

  Generic: takes any `Oracle.Sources.Streams` adapter (Binance, OKX,
  Bybit, etc.), holds one WebSocket to the adapter's endpoint,
  subscribes to the requested channels, and dispatches parsed feed
  structs (Ticker / Trade / BookDelta / Liquidation) to a caller-
  supplied target pid.

  Application-level ping (used by exchanges like OKX that require
  JSON `{"op":"ping"}` heartbeats separate from WebSocket protocol
  PINGs) is scheduled here. WebSocket protocol PINGs are handled by
  Gun natively — we never see them.

  Reconnect uses exponential backoff bounded to [1s, 60s]. On every
  successful upgrade the backoff resets to 1s.
  """

  use GenServer

  require Logger

  @connect_timeout_ms 10_000
  @reconnect_min_ms 1_000
  @reconnect_max_ms 60_000

  defstruct [
    :adapter,
    :channels,
    :target,
    :tag,
    :uri,
    :gun_pid,
    :gun_mref,
    :ws_ref,
    :ping_timer,
    :ping_message,
    :ping_interval_ms,
    backoff_ms: @reconnect_min_ms,
    conn_state: :disconnected
  ]

  @type start_opt ::
          {:adapter, module()}
          | {:channels, [Oracle.Sources.Streams.channel()]}
          | {:target, pid()}
          | {:tag, atom()}
          | {:name, GenServer.name()}

  @spec start_link([start_opt()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, name_opts)
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    adapter = Keyword.fetch!(opts, :adapter)
    channels = Keyword.fetch!(opts, :channels)
    target = Keyword.fetch!(opts, :target)
    tag = Keyword.get(opts, :tag, :oracle_tick)

    url = adapter.ws_url(channels)

    case parse_ws_url(url) do
      {:ok, uri} ->
        state =
          %__MODULE__{
            adapter: adapter,
            channels: channels,
            target: target,
            tag: tag,
            uri: uri
          }
          |> apply_ping_config(adapter.ping_config())

        send(self(), :connect)
        {:ok, state}

      {:error, reason} ->
        {:stop, {:invalid_ws_url, url, reason}}
    end
  end

  # --- Connect / upgrade (async via Gun) ---

  @impl true
  def handle_info(:connect, %{uri: uri} = state) do
    open_opts = %{
      protocols: [:http],
      transport: transport_for(uri.scheme),
      tls_opts: tls_opts(uri.host),
      retry: 0,
      connect_timeout: @connect_timeout_ms
    }

    case :gun.open(to_charlist(uri.host), uri.port, open_opts) do
      {:ok, gun_pid} ->
        mref = Process.monitor(gun_pid)
        {:noreply, %{state | gun_pid: gun_pid, gun_mref: mref, conn_state: :opening}}

      {:error, reason} ->
        Logger.info(
          "StreamConn: gun.open failed, adapter=#{inspect(state.adapter)}, reason=#{inspect(reason)}"
        )

        schedule_reconnect(state)
    end
  end

  # TCP/TLS came up — upgrade to WebSocket
  def handle_info({:gun_up, gun_pid, _protocol}, %{gun_pid: gun_pid, uri: uri} = state) do
    ws_ref = :gun.ws_upgrade(gun_pid, to_charlist(uri.path))
    {:noreply, %{state | ws_ref: ws_ref, conn_state: :upgrading}}
  end

  # WebSocket handshake succeeded — subscribe and start pinging
  def handle_info(
        {:gun_upgrade, gun_pid, ws_ref, ["websocket"], _headers},
        %{gun_pid: gun_pid, ws_ref: ws_ref} = state
      ) do
    send_subscribes(state)

    {:noreply,
     %{state | conn_state: :ready, backoff_ms: @reconnect_min_ms} |> schedule_ping()}
  end

  # WebSocket handshake rejected
  def handle_info(
        {:gun_response, gun_pid, ws_ref, _fin, status, _headers},
        %{gun_pid: gun_pid, ws_ref: ws_ref} = state
      ) do
    Logger.info(
      "StreamConn: upgrade rejected, adapter=#{inspect(state.adapter)}, status=#{status}"
    )

    teardown(state)
    schedule_reconnect(state)
  end

  # Stream-level error during upgrade
  def handle_info(
        {:gun_error, gun_pid, ws_ref, reason},
        %{gun_pid: gun_pid, ws_ref: ws_ref} = state
      ) do
    Logger.info(
      "StreamConn: gun stream error, adapter=#{inspect(state.adapter)}, reason=#{inspect(reason)}"
    )

    teardown(state)
    schedule_reconnect(state)
  end

  # Frame from the exchange
  def handle_info(
        {:gun_ws, gun_pid, ws_ref, frame},
        %{gun_pid: gun_pid, ws_ref: ws_ref} = state
      ) do
    handle_frame(frame, state)
  end

  # Connection dropped (TCP/TLS closed, remote side)
  def handle_info(
        {:gun_down, gun_pid, _protocol, reason, _killed_streams},
        %{gun_pid: gun_pid} = state
      ) do
    Logger.info(
      "StreamConn: gun_down, adapter=#{inspect(state.adapter)}, reason=#{inspect(reason)}"
    )

    teardown(state)
    schedule_reconnect(state)
  end

  # Gun process itself exited
  def handle_info(
        {:DOWN, mref, :process, gun_pid, reason},
        %{gun_mref: mref, gun_pid: gun_pid} = state
      ) do
    Logger.info(
      "StreamConn: gun exited, adapter=#{inspect(state.adapter)}, reason=#{inspect(reason)}"
    )

    schedule_reconnect(%{
      state
      | gun_pid: nil,
        gun_mref: nil,
        ws_ref: nil,
        conn_state: :disconnected
    })
  end

  # Application-level ping tick
  def handle_info(
        :ping,
        %{gun_pid: pid, ws_ref: ref, ping_message: msg, conn_state: :ready} = state
      )
      when not is_nil(pid) and not is_nil(msg) do
    :gun.ws_send(pid, ref, {:text, Jason.encode!(msg)})
    {:noreply, schedule_ping(state)}
  end

  # Ping timer fired while disconnected — drop it
  def handle_info(:ping, state), do: {:noreply, state}

  # Late messages from a previous incarnation of the connection
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: teardown(state)

  # --- Frame handling ---

  defp handle_frame({:text, text}, state) do
    case Jason.decode(text) do
      {:ok, decoded} -> dispatch(state.adapter.parse_message(decoded), state)
      _ -> :ok
    end

    {:noreply, state}
  end

  defp handle_frame({:binary, _bin}, state), do: {:noreply, state}

  defp handle_frame(:close, state), do: reconnect_after_close(state)
  defp handle_frame({:close, _code}, state), do: reconnect_after_close(state)
  defp handle_frame({:close, _code, _reason}, state), do: reconnect_after_close(state)

  defp handle_frame(_other, state), do: {:noreply, state}

  defp reconnect_after_close(state) do
    teardown(state)
    schedule_reconnect(state)
  end

  defp dispatch({:ok, structs}, state) when is_list(structs) do
    Enum.each(structs, &send(state.target, {state.tag, state.adapter.name(), &1}))
  end

  defp dispatch(_, _state), do: :ok

  # --- Subscribes ---

  defp send_subscribes(%{
         gun_pid: pid,
         ws_ref: ref,
         adapter: adapter,
         channels: channels
       }) do
    adapter.subscribe_messages(channels)
    |> Enum.each(fn msg -> :gun.ws_send(pid, ref, {:text, Jason.encode!(msg)}) end)
  end

  # --- Reconnect + teardown ---

  defp schedule_reconnect(state) do
    Process.send_after(self(), :connect, state.backoff_ms)
    next_backoff = min(state.backoff_ms * 2, @reconnect_max_ms)

    {:noreply,
     %{
       state
       | conn_state: :disconnected,
         gun_pid: nil,
         gun_mref: nil,
         ws_ref: nil,
         backoff_ms: next_backoff
     }}
  end

  defp teardown(state) do
    if state.ping_timer, do: Process.cancel_timer(state.ping_timer)
    if state.gun_mref, do: Process.demonitor(state.gun_mref, [:flush])
    if state.gun_pid, do: :gun.close(state.gun_pid)
    :ok
  end

  # --- Ping (application-level; WebSocket-protocol PINGs are Gun's job) ---

  defp apply_ping_config(state, nil), do: state

  defp apply_ping_config(state, {message, interval_ms}) do
    %{state | ping_message: message, ping_interval_ms: interval_ms}
  end

  defp schedule_ping(%{ping_message: nil} = state), do: state

  defp schedule_ping(%{ping_interval_ms: interval, ping_timer: old} = state) do
    if old, do: Process.cancel_timer(old)
    timer = Process.send_after(self(), :ping, interval)
    %{state | ping_timer: timer}
  end

  # --- Transport helpers ---

  defp transport_for(:wss), do: :tls
  defp transport_for(:ws), do: :tcp

  defp tls_opts(host) do
    [
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      depth: 3,
      server_name_indication: to_charlist(host),
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end

  # --- URL parsing ---

  defp parse_ws_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} = uri when scheme in ["ws", "wss"] and is_binary(host) ->
        port = uri.port || if(scheme == "wss", do: 443, else: 80)
        path = uri.path || "/"

        query =
          case uri.query do
            nil -> ""
            q -> "?" <> q
          end

        {:ok,
         %{
           scheme: String.to_existing_atom(scheme),
           host: host,
           port: port,
           path: path <> query
         }}

      _ ->
        {:error, :invalid_url}
    end
  end
end
