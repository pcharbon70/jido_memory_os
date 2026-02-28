# Phase 7 SLO Baseline

These SLOs are locked for the Phase 7 benchmark harness in `Jido.MemoryOS.Performance`.

## Latency SLOs (milliseconds)
- `remember`: p50 <= 120, p95 <= 600, p99 <= 900
- `retrieve`: p50 <= 120, p95 <= 650, p99 <= 1000
- `mixed`: p50 <= 150, p95 <= 700, p99 <= 1100

## Throughput SLOs (operations/sec)
- `remember`: >= 10
- `retrieve`: >= 10
- `mixed`: >= 6

## Error Budget
- transient error rate <= 20%
- permanent error rate <= 5%

## Benchmark Workloads
- Ingestion-heavy: `Performance.benchmark_ingestion/4`
- Retrieval-heavy: `Performance.benchmark_retrieval/4`
- Mixed read/write/consolidate: `Performance.benchmark_mixed/4`
