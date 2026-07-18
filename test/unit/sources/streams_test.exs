defmodule Oracle.Sources.StreamsTest do
  use ExUnit.Case, async: true

  defmodule MockStream do
    @behaviour Oracle.Sources.Streams

    @impl true
    def name, do: :mock

    @impl true
    def ws_url(_channels), do: "wss://mock.example.com/ws"

    @impl true
    def subscribe_messages(channels) do
      [%{"type" => "subscribe", "channels" => Enum.map(channels, & &1.feed)}]
    end

    @impl true
    def unsubscribe_messages(channels) do
      [%{"type" => "unsubscribe", "channels" => Enum.map(channels, & &1.feed)}]
    end

    @impl true
    def parse_message(%{"type" => "ticker"}), do: {:ok, []}
    def parse_message(%{"type" => "ping"}), do: :ping
    def parse_message(%{"type" => "info"}), do: :ignore
    def parse_message(_), do: {:error, :unknown_message}

    @impl true
    def ping_config, do: {%{"op" => "ping"}, 30_000}

    @impl true
    def supported_feeds, do: [:ticker, :trades, :book]
  end

  describe "behaviour implementation" do
    test "name returns an atom" do
      assert MockStream.name() == :mock
    end

    test "ws_url returns a string" do
      url = MockStream.ws_url([%{feed: :ticker, pair: :btc_usdt}])
      assert is_binary(url)
      assert String.starts_with?(url, "wss://")
    end

    test "subscribe_messages returns list of maps" do
      channels = [%{feed: :ticker, pair: :btc_usdt}]
      msgs = MockStream.subscribe_messages(channels)
      assert is_list(msgs)
      assert Enum.all?(msgs, &is_map/1)
    end

    test "unsubscribe_messages returns list of maps" do
      channels = [%{feed: :ticker, pair: :btc_usdt}]
      msgs = MockStream.unsubscribe_messages(channels)
      assert is_list(msgs)
      assert Enum.all?(msgs, &is_map/1)
    end

    test "parse_message returns {:ok, list}" do
      assert {:ok, []} = MockStream.parse_message(%{"type" => "ticker"})
    end

    test "parse_message returns :ping" do
      assert :ping = MockStream.parse_message(%{"type" => "ping"})
    end

    test "parse_message returns :ignore" do
      assert :ignore = MockStream.parse_message(%{"type" => "info"})
    end

    test "parse_message returns {:error, term}" do
      assert {:error, :unknown_message} = MockStream.parse_message(%{"type" => "bad"})
    end

    test "ping_config returns tuple or nil" do
      {msg, interval} = MockStream.ping_config()
      assert is_map(msg)
      assert is_integer(interval)
    end

    test "supported_feeds returns list of atoms" do
      feeds = MockStream.supported_feeds()
      assert is_list(feeds)
      assert :ticker in feeds
      assert :trades in feeds
      assert :book in feeds
    end
  end
end
