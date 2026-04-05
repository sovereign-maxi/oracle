defmodule Oracle.Connection do
  @moduledoc """
  Stateless connection management utilities.

  Pure functions for WebSocket connection decisions. Applications pass
  state in and get decisions back — no state is held here.

  ## Usage

      # Calculate backoff delay
      delay = Oracle.Connection.backoff_ms(attempt, base_ms: 1_000, max_ms: 30_000)

      # Assess connection health
      status = Oracle.Connection.assess_health(metrics, thresholds)

      # Decide whether to reconnect
      case Oracle.Connection.reconnect_action(attempt, reason, max_attempts: 20) do
        {:reconnect, delay_ms} -> schedule_reconnect(delay_ms)
        :give_up -> stop_connection()
      end
  """

  @default_base_ms 1_000
  @default_max_ms 30_000
  @default_max_attempts 20

  @doc """
  Calculates exponential backoff delay with jitter.

  ## Options

  - `:base_ms` - Base delay in milliseconds (default: 1000)
  - `:max_ms` - Maximum delay in milliseconds (default: 30000)

  ## Examples

      iex> Oracle.Connection.backoff_ms(0)
      ms when ms >= 500 and ms <= 1500

      iex> Oracle.Connection.backoff_ms(5, base_ms: 1_000, max_ms: 30_000)
      ms when ms <= 30_000
  """
  @spec backoff_ms(non_neg_integer(), keyword()) :: non_neg_integer()
  def backoff_ms(attempt, opts \\ []) do
    base_ms = Keyword.get(opts, :base_ms, @default_base_ms)
    max_ms = Keyword.get(opts, :max_ms, @default_max_ms)

    # Exponential: base * 2^attempt
    exponential = base_ms * Integer.pow(2, min(attempt, 15))

    # Cap at max
    capped = min(exponential, max_ms)

    # Add jitter: +/- 50%
    jitter_range = max(div(capped, 2), 1)
    jitter = :rand.uniform(jitter_range * 2) - jitter_range

    max(capped + jitter, 0)
  end

  @doc """
  Assesses connection health based on metrics.

  ## Metrics Map

  Expected keys:
  - `:message_rate` - Messages per second
  - `:last_message_age_ms` - Milliseconds since last message
  - `:error_rate` - Errors per minute

  ## Options

  - `:stale_ms` - Milliseconds before considering stale (default: 30_000)
  - `:min_rate` - Minimum acceptable message rate (default: 0.1)
  - `:max_error_rate` - Maximum acceptable error rate (default: 10)

  ## Returns

  - `:healthy` - All metrics within acceptable bounds
  - `:degraded` - Some metrics approaching thresholds
  - `:unhealthy` - Metrics exceed thresholds
  """
  @spec assess_health(map(), keyword()) :: :healthy | :degraded | :unhealthy
  def assess_health(metrics, opts \\ []) do
    stale_ms = Keyword.get(opts, :stale_ms, 30_000)
    min_rate = Keyword.get(opts, :min_rate, 0.1)
    max_error_rate = Keyword.get(opts, :max_error_rate, 10)

    last_age = Map.get(metrics, :last_message_age_ms, 0)
    msg_rate = Map.get(metrics, :message_rate, 1.0)
    error_rate = Map.get(metrics, :error_rate, 0)

    cond do
      last_age > stale_ms * 2 -> :unhealthy
      error_rate > max_error_rate * 2 -> :unhealthy
      msg_rate < min_rate / 2 -> :unhealthy
      last_age > stale_ms -> :degraded
      error_rate > max_error_rate -> :degraded
      msg_rate < min_rate -> :degraded
      true -> :healthy
    end
  end

  @doc """
  Decides whether to reconnect or give up.

  ## Options

  - `:max_attempts` - Maximum reconnection attempts (default: 20)
  - `:base_ms` - Base backoff delay (default: 1000)
  - `:max_ms` - Maximum backoff delay (default: 30000)
  - `:fatal_reasons` - List of reasons that should immediately give up (default: [])

  ## Returns

  - `{:reconnect, delay_ms}` - Should reconnect after delay
  - `:give_up` - Should stop trying to reconnect
  """
  @spec reconnect_action(non_neg_integer(), term(), keyword()) ::
          {:reconnect, non_neg_integer()} | :give_up
  def reconnect_action(attempt, reason, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    fatal_reasons = Keyword.get(opts, :fatal_reasons, [])

    cond do
      reason in fatal_reasons -> :give_up
      attempt >= max_attempts -> :give_up
      true -> {:reconnect, backoff_ms(attempt, opts)}
    end
  end
end
