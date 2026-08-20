#pragma once
#include <cstdint>

// A single market data update -- one price/volume observation, the same
// shape an exchange feed would send out. `timestamp` is just an
// increasing counter here rather than a wall-clock time, to keep the
// simulation deterministic and easy to reason about.
struct Tick {
    uint64_t timestamp;
    double price;
    uint32_t volume;
};
