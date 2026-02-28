defmodule Jido.MemoryOS.Performance do
  @moduledoc """
  Benchmark and SLO helpers for Phase 7 performance validation.
  """

  @type benchmark_result :: %{
          count: non_neg_integer(),
          success: non_neg_integer(),
          errors: non_neg_integer(),
          elapsed_ms: non_neg_integer(),
          throughput_per_sec: float(),
          p50_ms: float(),
          p95_ms: float(),
          p99_ms: float(),
          latencies_ms: [float()]
        }

  @spec default_slos() :: map()
  def default_slos do
    %{
      latency_ms: %{
        remember: %{p50: 120.0, p95: 600.0, p99: 900.0},
        retrieve: %{p50: 120.0, p95: 650.0, p99: 1_000.0},
        mixed: %{p50: 150.0, p95: 700.0, p99: 1_100.0}
      },
      throughput_per_sec: %{
        remember: 10.0,
        retrieve: 10.0,
        mixed: 6.0
      },
      error_budget: %{
        transient_rate_max: 0.2,
        permanent_rate_max: 0.05
      }
    }
  end

  @spec benchmark_ingestion(map() | struct(), non_neg_integer(), pos_integer(), keyword()) ::
          benchmark_result()
  def benchmark_ingestion(target, count, concurrency, opts \\ []) do
    op = fn idx ->
      Jido.MemoryOS.remember(
        target,
        %{
          class: :episodic,
          kind: :event,
          text: "perf ingest #{idx}",
          chain_id: "chain:perf:ingest"
        },
        opts
      )
    end

    run_benchmark(count, concurrency, op)
  end

  @spec benchmark_retrieval(map() | struct(), non_neg_integer(), pos_integer(), keyword()) ::
          benchmark_result()
  def benchmark_retrieval(target, count, concurrency, opts \\ []) do
    op = fn idx ->
      Jido.MemoryOS.retrieve(
        target,
        %{text_contains: "perf", tier_mode: :short, limit: 5, debug: rem(idx, 3) == 0},
        opts
      )
    end

    run_benchmark(count, concurrency, op)
  end

  @spec benchmark_mixed(map() | struct(), non_neg_integer(), pos_integer(), keyword()) ::
          benchmark_result()
  def benchmark_mixed(target, count, concurrency, opts \\ []) do
    op = fn idx ->
      case rem(idx, 5) do
        0 ->
          Jido.MemoryOS.consolidate(target, opts)

        1 ->
          Jido.MemoryOS.retrieve(
            target,
            %{text_contains: "perf", tier_mode: :short, limit: 3},
            opts
          )

        _ ->
          Jido.MemoryOS.remember(
            target,
            %{
              class: :episodic,
              kind: :event,
              text: "perf mixed #{idx}",
              chain_id: "chain:perf:mixed"
            },
            opts
          )
      end
    end

    run_benchmark(count, concurrency, op)
  end

  @spec run_benchmark(non_neg_integer(), pos_integer(), (non_neg_integer() -> term())) ::
          benchmark_result()
  def run_benchmark(0, _concurrency, _op) do
    %{
      count: 0,
      success: 0,
      errors: 0,
      elapsed_ms: 0,
      throughput_per_sec: 0.0,
      p50_ms: 0.0,
      p95_ms: 0.0,
      p99_ms: 0.0,
      latencies_ms: []
    }
  end

  def run_benchmark(count, concurrency, op) when count >= 0 and concurrency > 0 do
    started = System.monotonic_time(:microsecond)

    results =
      1..count
      |> Task.async_stream(
        fn idx ->
          op_started = System.monotonic_time(:microsecond)
          result = op.(idx)
          op_finished = System.monotonic_time(:microsecond)
          latency_ms = microseconds_to_ms(op_finished - op_started)

          %{result: result, latency_ms: latency_ms}
        end,
        max_concurrency: concurrency,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn
        {:ok, entry} -> entry
        {:exit, reason} -> %{result: {:error, reason}, latency_ms: 0.0}
      end)

    finished = System.monotonic_time(:microsecond)
    elapsed_ms = max(1, microseconds_to_ms(finished - started))

    latencies = Enum.map(results, & &1.latency_ms)
    success = Enum.count(results, &match?({:ok, _}, &1.result))
    errors = count - success

    %{
      count: count,
      success: success,
      errors: errors,
      elapsed_ms: round(elapsed_ms),
      throughput_per_sec: throughput_per_sec(count, elapsed_ms),
      p50_ms: percentile(latencies, 0.50),
      p95_ms: percentile(latencies, 0.95),
      p99_ms: percentile(latencies, 0.99),
      latencies_ms: latencies
    }
  end

  @spec percentile([number()], float()) :: float()
  def percentile([], _quantile), do: 0.0

  def percentile(values, quantile) when is_list(values) do
    sorted = Enum.sort(values)
    index = floor((length(sorted) - 1) * clamp01(quantile))

    sorted
    |> Enum.at(index, 0)
    |> normalize_float()
  end

  @spec throughput_per_sec(non_neg_integer(), number()) :: float()
  def throughput_per_sec(0, _elapsed_ms), do: 0.0

  def throughput_per_sec(count, elapsed_ms) when elapsed_ms > 0 do
    count / (elapsed_ms / 1_000.0)
  end

  @spec clamp01(number()) :: float()
  defp clamp01(value) when value <= 0, do: 0.0
  defp clamp01(value) when value >= 1, do: 1.0
  defp clamp01(value), do: normalize_float(value)

  @spec microseconds_to_ms(integer()) :: float()
  defp microseconds_to_ms(us), do: normalize_float(us / 1_000.0)

  @spec normalize_float(number()) :: float()
  defp normalize_float(value), do: value * 1.0
end
