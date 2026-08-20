# Market Data Pipeline (Producer-Consumer, C++17)

A minimal simulation of how real market data infrastructure is
structured: one thread generates a stream of price ticks (like an
exchange feed), a second thread consumes them and computes running
trading indicators (SMA, EMA, VWAP), and a thread-safe queue with a
mutex + condition_variable connects the two safely.

## Why this pattern, and why it's relevant

Real trading systems separate **feed handling** (reading data off the
wire as fast as possible) from **strategy/processing logic** (doing
something with that data), specifically so a slow calculation never
causes the feed handler to fall behind and drop or delay incoming
data. This project is a small, honest version of that same idea.

## Build & run

```
mkdir build && cd build
cmake ..
make
./market_pipeline
```

Or directly with g++ (note `-pthread` is required):
```
g++ -std=c++17 -Wall -Wextra -Iinclude -pthread src/main.cpp -o market_pipeline
./market_pipeline
```

Tested with g++ 13 on Ubuntu 24.04. No external dependencies.

### Verifying there are no race conditions
This was built and run under **ThreadSanitizer** during development,
which found zero data races:
```
g++ -std=c++17 -Iinclude -pthread -fsanitize=thread -g src/main.cpp -o market_pipeline_tsan
./market_pipeline_tsan
```
Worth mentioning in an interview: it shows you know that "it ran
without crashing" isn't proof of thread safety, and that there's a
real tool for checking.

## How it works, in order

1. `main()` creates one shared `SafeQueue<Tick>` and launches two
   `std::thread`s on it: `producerThread` and `consumerThread`.
2. **Producer**: generates a random-walk price series, wraps each
   price into a `Tick{timestamp, price, volume}`, and calls
   `queue.push(tick)`. A small `sleep_for` between ticks simulates data
   arriving over time rather than all at once. When done, it calls
   `queue.shutdown()`.
3. **Consumer**: loops calling `queue.pop()`, which blocks until a
   tick is available. For each tick it updates three running
   indicators (below) and prints a snapshot every 10th tick.
4. `queue.pop()` returns `std::nullopt` once the queue has been shut
   down *and* fully drained — that's the consumer's signal to exit its
   loop.
5. `main()` calls `.join()` on both threads so the program doesn't
   exit while either thread is still running.

## The concurrency primitives, explained plainly

- **`std::mutex`** — a lock. Only one thread can hold it at a time.
  Used here so the producer and consumer never touch the underlying
  `std::queue` at the same instant (which would corrupt it).
- **`std::condition_variable`** — lets a thread sleep until another
  thread tells it "something changed, check again." Without it, the
  consumer would have to loop constantly asking "is there data yet?"
  (busy-waiting), wasting a full CPU core. `cv_.wait(lock, predicate)`
  sleeps until `notify_one()`/`notify_all()` is called *and* the
  predicate is true.
- **Spurious wakeup** — `condition_variable::wait` can, on some
  platforms, wake up even though nobody called notify. Passing a
  predicate to `wait()` handles this automatically: on any wakeup it
  re-checks the real condition and goes back to sleep if it's not
  actually true yet.
- **`std::ref`** — `std::thread` copies its arguments by default.
  `std::ref(queue)` passes the *same* queue object to both threads
  instead of giving each one its own independent copy.
- **`.join()`** — blocks the calling thread (here, `main`) until the
  target thread finishes. Without it, `main` could return and destroy
  the queue while a thread is still using it.

## Financial terms glossary (all you need for this project)

- **Tick** — a single price update from the market (here:
  timestamp + price + volume).
- **SMA (Simple Moving Average)** — the average price over the last N
  ticks. Smooths out noise; lags behind sudden moves because it treats
  every tick in the window equally.
- **EMA (Exponential Moving Average)** — like SMA, but weights recent
  prices more heavily using a smoothing factor (`alpha`), so it reacts
  faster to new information.
- **VWAP (Volume-Weighted Average Price)** — the average price weighted
  by how much volume traded at each price. A trade of 50 units moves
  VWAP more than a trade of 5 units at the same price. Traders use it
  as a benchmark for "was my execution price good or bad."
- **Market data feed** — the live stream of ticks an exchange
  publishes; what the producer thread here is simulating.

## Study plan for one day

1. **Trace the SafeQueue first** (30-45 min) — it's the part with real
   concurrency logic. Read `push()` and `pop()` until you can explain,
   without looking, why the mutex is needed and why `wait()` takes a
   predicate instead of just checking `if (queue_.empty())`.
2. **Trace the consumer's math** (20 min) — SMA/EMA/VWAP are each 3-4
   lines; make sure you could derive the VWAP formula from the
   definition above without looking at the code.
3. **Run it, then break it on purpose**: comment out the
   `cv_.notify_one()` call in `push()` and predict what happens before
   running (the consumer sleeps forever after processing the current
   backlog — a "missed wakeup" bug). This single experiment will make
   the condition_variable click far better than reading about it.
4. **Rehearse likely interview questions**:
   - "What happens if you remove the mutex?" → data race on
     `queue_`, corrupted state, undefined behavior.
   - "What if the consumer is slower than the producer?" → the queue
     grows unbounded in this simple version; a production system would
     cap its size or drop/aggregate old ticks (a "next steps" answer).
   - "Why join() instead of detach()?" → so `main` doesn't exit (and
     destroy the queue) while a thread is still using it.

## Possible extensions (good "what would you add" answers)

- Bound the queue size and have `push()` block when full (classic
  bounded producer-consumer), instead of growing without limit.
- Multiple producer threads simulating several instruments at once.
- Graceful shutdown triggered by a signal (Ctrl+C) instead of a fixed
  tick count.
- Swap the mutex-based queue for a lock-free ring buffer and discuss
  why that matters for latency-sensitive systems.
