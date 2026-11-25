// market_pipeline.cpp
//
// A multithreaded producer-consumer market data aggregation pipeline.
//
// Simulates a stream of price "ticks" for a fixed set of symbols. Multiple
// producer threads generate ticks concurrently and push them into a bounded,
// thread-safe queue. Multiple consumer threads pop ticks off the queue and
// update a rolling volume-weighted average price (VWAP) per symbol.
//
// The program benchmarks throughput (ticks/sec) for the multithreaded
// pipeline and compares it against a single-threaded baseline that does the
// exact same amount of work with no concurrency.
//
// Build:   g++ -O2 -std=c++17 -pthread main.cpp -o market_pipeline
// Run:     ./market_pipeline

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// Tick: a single simulated market data event.
// ---------------------------------------------------------------------------
struct Tick {
    int symbolId;
    double price;
    uint32_t volume;
};

// ---------------------------------------------------------------------------
// BoundedQueue: a thread-safe, fixed-capacity queue using a mutex and two
// condition variables (one to signal "not empty", one to signal "not full").
// Bounding the queue applies backpressure so producers can't run arbitrarily
// far ahead of consumers and blow up memory.
// ---------------------------------------------------------------------------
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    // Blocks if the queue is full until space is available (or shutdown).
    void push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        notFull_.wait(lock, [this] { return queue_.size() < capacity_ || shutdown_; });
        if (shutdown_) return;
        queue_.push(std::move(item));
        lock.unlock();
        notEmpty_.notify_one();
    }

    // Blocks if the queue is empty until an item is available (or shutdown).
    // Returns false if the queue is shut down and drained (time to exit).
    bool pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [this] { return !queue_.empty() || shutdown_; });
        if (queue_.empty()) return false;  // shutdown_ and nothing left
        out = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        notFull_.notify_one();
        return true;
    }

    // Signals all waiting threads to wake up and exit once drained.
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        notEmpty_.notify_all();
        notFull_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
    std::queue<T> queue_;
    size_t capacity_;
    bool shutdown_ = false;
};

// ---------------------------------------------------------------------------
// SymbolAggregator: maintains a running VWAP per symbol.
// VWAP = sum(price * volume) / sum(volume)
// Each symbol gets its own mutex so consumer threads updating different
// symbols don't contend with each other (finer-grained locking than one
// global mutex for the whole aggregator).
// ---------------------------------------------------------------------------
class SymbolAggregator {
public:
    explicit SymbolAggregator(int numSymbols)
        : locks_(numSymbols), notionalSum_(numSymbols, 0.0), volumeSum_(numSymbols, 0) {}

    void update(const Tick& t) {
        std::lock_guard<std::mutex> lock(locks_[t.symbolId]);
        notionalSum_[t.symbolId] += t.price * t.volume;
        volumeSum_[t.symbolId] += t.volume;
    }

    double vwap(int symbolId) {
        std::lock_guard<std::mutex> lock(locks_[symbolId]);
        return volumeSum_[symbolId] == 0 ? 0.0 : notionalSum_[symbolId] / volumeSum_[symbolId];
    }

private:
    std::vector<std::mutex> locks_;
    std::vector<double> notionalSum_;
    std::vector<uint64_t> volumeSum_;
};

// ---------------------------------------------------------------------------
// Tick generation: shared logic so both the multithreaded and single
// threaded baseline do identical work per tick.
// ---------------------------------------------------------------------------
Tick makeRandomTick(std::mt19937& rng, int numSymbols) {
    static thread_local std::uniform_int_distribution<int> symbolDist(0, numSymbols - 1);
    static thread_local std::uniform_real_distribution<double> priceDist(50.0, 500.0);
    static thread_local std::uniform_int_distribution<uint32_t> volumeDist(1, 1000);

    Tick t;
    t.symbolId = symbolDist(rng);
    t.price = priceDist(rng);
    t.volume = volumeDist(rng);
    return t;
}

// ---------------------------------------------------------------------------
// Multithreaded pipeline: N producer threads generate ticks in local batches
// and push whole batches into a shared bounded queue; M consumer threads pop
// batches and update the aggregator.
//
// Batching matters: with a single mutex+condvar per tick, lock acquisition
// overhead can dominate the (tiny) cost of updating one aggregator entry,
// especially once thread count exceeds available cores. Pushing/popping in
// batches of BATCH_SIZE amortizes that per-item synchronization cost across
// many ticks per lock acquisition, which is the standard first fix before
// reaching for something like a lock-free queue.
// ---------------------------------------------------------------------------
constexpr int BATCH_SIZE = 256;

double runMultithreaded(int numProducers, int numConsumers, int ticksPerProducer,
                         int numSymbols, SymbolAggregator& agg) {
    BoundedQueue<std::vector<Tick>> queue(256);
    std::atomic<int> producersRemaining{numProducers};

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> producers;
    for (int p = 0; p < numProducers; ++p) {
        producers.emplace_back([&, p] {
            std::mt19937 rng(p * 7919 + 1);  // distinct seed per producer thread
            std::vector<Tick> batch;
            batch.reserve(BATCH_SIZE);
            for (int i = 0; i < ticksPerProducer; ++i) {
                batch.push_back(makeRandomTick(rng, numSymbols));
                if (static_cast<int>(batch.size()) == BATCH_SIZE) {
                    queue.push(std::move(batch));
                    batch.clear();
                    batch.reserve(BATCH_SIZE);
                }
            }
            if (!batch.empty()) {
                queue.push(std::move(batch));
            }
            if (--producersRemaining == 0) {
                queue.shutdown();  // last producer done -> signal consumers to wind down
            }
        });
    }

    std::vector<std::thread> consumers;
    for (int c = 0; c < numConsumers; ++c) {
        consumers.emplace_back([&] {
            std::vector<Tick> batch;
            while (queue.pop(batch)) {
                for (const auto& t : batch) {
                    agg.update(t);
                }
            }
        });
    }

    for (auto& th : producers) th.join();
    for (auto& th : consumers) th.join();

    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

// ---------------------------------------------------------------------------
// Single-threaded baseline: generates and aggregates the same total number
// of ticks with no concurrency at all, for a fair throughput comparison.
// ---------------------------------------------------------------------------
double runSingleThreaded(int totalTicks, int numSymbols, SymbolAggregator& agg) {
    std::mt19937 rng(12345);

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < totalTicks; ++i) {
        Tick t = makeRandomTick(rng, numSymbols);
        agg.update(t);
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

int main() {
    const int numSymbols = 10;
    const int numProducers = 4;
    const int numConsumers = 4;
    const int ticksPerProducer = 500000;
    const int totalTicks = numProducers * ticksPerProducer;

    std::cout << "Market Data Pipeline Benchmark\n";
    std::cout << "================================\n";
    std::cout << "Symbols: " << numSymbols << " | Total ticks: " << totalTicks << "\n";
    std::cout << "Producers: " << numProducers << " | Consumers: " << numConsumers << "\n\n";

    // --- Multithreaded run ---
    SymbolAggregator mtAgg(numSymbols);
    double mtSeconds = runMultithreaded(numProducers, numConsumers, ticksPerProducer,
                                         numSymbols, mtAgg);
    double mtThroughput = totalTicks / mtSeconds;

    std::cout << "[Multithreaded]\n";
    std::cout << "  Time:       " << std::fixed << std::setprecision(4) << mtSeconds << " s\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << mtThroughput
              << " ticks/sec\n\n";

    // --- Single-threaded baseline ---
    SymbolAggregator stAgg(numSymbols);
    double stSeconds = runSingleThreaded(totalTicks, numSymbols, stAgg);
    double stThroughput = totalTicks / stSeconds;

    std::cout << "[Single-threaded baseline]\n";
    std::cout << "  Time:       " << std::fixed << std::setprecision(4) << stSeconds << " s\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << stThroughput
              << " ticks/sec\n\n";

    std::cout << "Speedup: " << std::fixed << std::setprecision(2) << (stSeconds / mtSeconds)
              << "x\n\n";

    // --- Sanity check: print VWAP per symbol from the multithreaded run ---
    std::cout << "Per-symbol VWAP (multithreaded run):\n";
    for (int s = 0; s < numSymbols; ++s) {
        std::cout << "  Symbol " << s << ": $" << std::fixed << std::setprecision(2)
                  << mtAgg.vwap(s) << "\n";
    }

    return 0;
}
