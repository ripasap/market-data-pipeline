#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>

// A thread-safe queue used to hand ticks from the producer thread
// (simulated market data feed) to the consumer thread (stats engine).
//
// WHY A MUTEX:
// The producer calls push() and the consumer calls pop() from two
// different threads, potentially at the exact same instant. std::queue
// itself has no protection against that -- two simultaneous writes (or
// a read during a write) corrupt its internal state. The mutex ensures
// only one thread touches the underlying queue at a time.
//
// WHY A CONDITION_VARIABLE:
// Without one, the consumer would have to sit in a tight loop
// repeatedly checking "is the queue empty yet?" (busy-waiting), which
// burns a full CPU core doing nothing useful. A condition_variable lets
// the consumer thread actually sleep until the producer explicitly
// wakes it up via notify_one().
template <typename T>
class SafeQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(value));
        }
        cv_.notify_one(); // wake the consumer if it's sleeping
    }

    // Blocks until an item is available or the queue has been shut down
    // and drained. Returns std::nullopt only in that second case, which
    // is the consumer's signal that it's safe to exit its loop.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);

        // wait() with a predicate is equivalent to a while-loop around
        // the condition, and that matters: condition_variable::wait can
        // return "spuriously" -- waking up with no corresponding
        // notify_one() call at all (this is allowed by the C++
        // standard on some platforms/implementations). Using the
        // predicate form means every wakeup re-checks the real
        // condition; a spurious wakeup just goes back to sleep instead
        // of incorrectly proceeding as if data were ready.
        cv_.wait(lock, [this] { return !queue_.empty() || shutdown_; });

        if (queue_.empty() && shutdown_) {
            return std::nullopt;
        }

        T value = std::move(queue_.front());
        queue_.pop();
        return value;
    }

    // Called once by the producer when there's no more data coming.
    // Wakes any thread waiting in pop() so the consumer can drain
    // whatever's left in the queue and then exit cleanly, instead of
    // pop() blocking forever with nothing left to wait for.
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        cv_.notify_all();
    }

private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool shutdown_ = false;
};
