defmodule Oracle.ConnectionTest do
  use ExUnit.Case, async: true

  alias Oracle.Connection

  describe "backoff_ms/2" do
    test "returns a positive integer" do
      ms = Connection.backoff_ms(0)
      assert is_integer(ms)
      assert ms >= 0
    end

    test "increases with attempts" do
      # Collect multiple samples to account for jitter
      low_samples = for _ <- 1..20, do: Connection.backoff_ms(0)
      high_samples = for _ <- 1..20, do: Connection.backoff_ms(5)

      avg_low = Enum.sum(low_samples) / length(low_samples)
      avg_high = Enum.sum(high_samples) / length(high_samples)
      assert avg_high > avg_low
    end

    test "respects max_ms" do
      ms = Connection.backoff_ms(20, max_ms: 5_000)
      assert ms <= 5_000 * 2
    end

    test "accepts custom base_ms" do
      samples = for _ <- 1..20, do: Connection.backoff_ms(0, base_ms: 100)
      avg = Enum.sum(samples) / length(samples)
      assert avg < 200
    end
  end

  describe "assess_health/2" do
    test "returns :healthy for good metrics" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 1_000, error_rate: 0}
      assert Connection.assess_health(metrics) == :healthy
    end

    test "returns :degraded for stale messages" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 35_000, error_rate: 0}
      assert Connection.assess_health(metrics) == :degraded
    end

    test "returns :unhealthy for very stale messages" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 65_000, error_rate: 0}
      assert Connection.assess_health(metrics) == :unhealthy
    end

    test "returns :degraded for high error rate" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 1_000, error_rate: 15}
      assert Connection.assess_health(metrics) == :degraded
    end

    test "returns :unhealthy for very high error rate" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 1_000, error_rate: 25}
      assert Connection.assess_health(metrics) == :unhealthy
    end

    test "returns :degraded for low message rate" do
      metrics = %{message_rate: 0.05, last_message_age_ms: 1_000, error_rate: 0}
      assert Connection.assess_health(metrics) == :degraded
    end

    test "accepts custom thresholds" do
      metrics = %{message_rate: 10.0, last_message_age_ms: 6_000, error_rate: 0}
      assert Connection.assess_health(metrics, stale_ms: 5_000) == :degraded
    end
  end

  describe "reconnect_action/3" do
    test "returns {:reconnect, ms} for normal attempts" do
      assert {:reconnect, ms} = Connection.reconnect_action(0, :closed)
      assert is_integer(ms)
      assert ms >= 0
    end

    test "returns :give_up when max attempts exceeded" do
      assert :give_up = Connection.reconnect_action(20, :closed)
    end

    test "returns :give_up for fatal reasons" do
      assert :give_up = Connection.reconnect_action(0, :auth_failed, fatal_reasons: [:auth_failed])
    end

    test "respects custom max_attempts" do
      assert {:reconnect, _} = Connection.reconnect_action(4, :closed, max_attempts: 5)
      assert :give_up = Connection.reconnect_action(5, :closed, max_attempts: 5)
    end
  end
end
