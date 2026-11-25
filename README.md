# Multithreaded Market Data Pipeline (C++)

A producer-consumer pipeline that simulates a stream of market data ticks
and aggregates a rolling volume-weighted average price (VWAP) per symbol,
using multiple producer and consumer threads coordinated through a bounded,
thread-safe queue.

## What it demonstrates

- **Custom thread-safe bounded queue** built from `std::mutex` +
  `std::condition_variable` (not `std::queue` alone — the wrapper adds
  blocking backpressure so producers can't outrun consumers unboundedly).
- **Producer-consumer concurrency**: N producer threads generate ticks,
  M consumer threads aggregate them, coordinated only through the queue.
- **Fine-grained locking**: the aggregator locks per-symbol, not globally,
  so consumer threads updating different symbols don't contend.
- **Batching as a performance fix**: the first (naive) version pushed and
  popped one tick per lock acquisition. Benchmarking showed synchronization
  overhead dominating the actual work per tick, so ticks are now pushed and
  popped in batches of 256, amortizing lock/condition-variable overhead
  across many items.
- **Benchmarking discipline**: throughput (ticks/sec) is measured for both
  the multithreaded pipeline and a single-threaded baseline doing identical
  work, so any speedup claim is backed by a real comparison instead of an
  assumption.

## Build

```
g++ -O2 -std=c++17 -pthread main.cpp -o market_pipeline
```

## Run

```
./market_pipeline
```

The program prints time and throughput for both runs, the speedup, and the
final VWAP per symbol as a sanity check.

## A note on the benchmark numbers

Results depend heavily on core count. On a single core, threads can only add
synchronization overhead, so the multithreaded run comes out *slower* than
the single-threaded baseline. That's expected, not a bug. With 4+ cores the
producers and consumers actually run in parallel, which is where the
pipeline is meant to pay off.

## Possible extensions

- Swap the mutex-based queue for a lock-free MPMC queue and re-benchmark.
- Add a rolling time-windowed VWAP (e.g., last 60 seconds) instead of an
  all-time running VWAP, using a ring buffer per symbol.
- Pin producer/consumer threads to specific cores and compare.
- Report p50/p99 tick-to-aggregation latency, not just aggregate throughput.
