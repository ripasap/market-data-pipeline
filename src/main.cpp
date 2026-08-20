// Producer-consumer market data pipeline.
//
// One thread ("producer") simulates an exchange feed: it generates a
// stream of price ticks and pushes them into a shared, thread-safe
// queue. A second thread ("consumer") simulates a strategy reading
// that feed: it pulls ticks off the queue as they arrive and updates
// running statistics (SMA, EMA, VWAP) -- the same three numbers a
// simple trading strategy might watch to decide when to act.
//
// This mirrors, in miniature, how real market data infrastructure is
// structured: a dedicated feed-handling thread that just moves data as
// fast as possible, decoupled from the processing thread that reacts
// to it, so a slow strategy calculation never blocks the feed handler
// from keeping up with the exchange.
#include "Tick.h"
#include "SafeQueue.h"

#include <thread>
#include <chrono>
#include <random>
#include <deque>
#include <iostream>
#include <iomanip>

// ---- Producer: simulates an exchange market data feed ----
void producerThread(SafeQueue<Tick>& queue, int numTicks) {
    std::mt19937 rng(42); // fixed seed -> reproducible run
    std::uniform_real_distribution<double> priceStep(-0.5, 0.5);
    std::uniform_int_distribution<int> volumeDist(1, 50);

    double price = 100.0;
    for (int i = 0; i < numTicks; ++i) {
        price += priceStep(rng);
        if (price < 1.0) price = 1.0; // keep the random walk sane

        Tick tick{static_cast<uint64_t>(i), price,
                  static_cast<uint32_t>(volumeDist(rng))};
        queue.push(tick);

        // Simulate ticks arriving over time rather than all at once.
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // No more data coming -- tell the queue so pop() can eventually
    // return nullopt instead of blocking forever.
    queue.shutdown();
}

// ---- Consumer: simulates a strategy consuming the feed ----
// Computes three standard indicators as ticks arrive:
//   SMA  (simple moving average)      -- average price over the last N ticks
//   EMA  (exponential moving average) -- like SMA but weights recent
//                                         prices more heavily, reacts faster
//   VWAP (volume-weighted avg price)  -- average price weighted by how
//                                         much volume traded at each price;
//                                         a large trade moves VWAP more
//                                         than a small one at the same price
void consumerThread(SafeQueue<Tick>& queue) {
    const size_t smaWindow = 10;
    std::deque<double> window;
    double windowSum = 0.0;

    double ema = 0.0;
    bool emaInitialized = false;
    const double emaAlpha = 2.0 / (smaWindow + 1); // standard EMA smoothing factor

    double vwapNotional = 0.0; // running sum of price * volume
    uint64_t vwapVolume = 0;   // running sum of volume

    size_t processed = 0;

    while (auto tickOpt = queue.pop()) {
        const Tick& t = *tickOpt;
        ++processed;

        // -- SMA: rolling sum over a fixed-size window --
        window.push_back(t.price);
        windowSum += t.price;
        if (window.size() > smaWindow) {
            windowSum -= window.front();
            window.pop_front();
        }
        double sma = windowSum / static_cast<double>(window.size());

        // -- EMA: exponential smoothing --
        if (!emaInitialized) {
            ema = t.price;
            emaInitialized = true;
        } else {
            ema = emaAlpha * t.price + (1.0 - emaAlpha) * ema;
        }

        // -- VWAP: running volume-weighted average --
        vwapNotional += t.price * t.volume;
        vwapVolume += t.volume;
        double vwap = vwapVolume > 0
                           ? vwapNotional / static_cast<double>(vwapVolume)
                           : 0.0;

        // Print every 10th tick (plus the first few) so the report is
        // readable instead of scrolling 100 lines.
        if (processed <= 3 || processed % 10 == 0) {
            std::cout << "tick #" << t.timestamp
                      << "  price=" << std::fixed << std::setprecision(2) << t.price
                      << "  vol=" << t.volume
                      << "  | SMA(" << smaWindow << ")=" << sma
                      << "  EMA=" << ema
                      << "  VWAP=" << vwap << "\n";
        }
    }

    std::cout << "\nconsumer finished: processed " << processed << " ticks.\n";
}

int main() {
    SafeQueue<Tick> queue;
    const int numTicks = 100;

    std::cout << "Starting producer (feed simulator) and consumer (stats engine)...\n\n";

    // std::ref is required here: std::thread copies its arguments by
    // default, and we need both threads operating on the SAME queue
    // object, not independent copies of it.
    std::thread producer(producerThread, std::ref(queue), numTicks);
    std::thread consumer(consumerThread, std::ref(queue));

    // join() blocks main() until each thread finishes. Without this,
    // main() could return and destroy the queue while the threads are
    // still using it -- undefined behavior.
    producer.join();
    consumer.join();

    std::cout << "\nBoth threads finished cleanly.\n";
    return 0;
}
