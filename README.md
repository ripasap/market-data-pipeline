# Market Data Pipeline (Producer-Consumer, C++17)

A minimal simulation of how real market data infrastructure is
structured: one thread generates a stream of price ticks (like an
exchange feed), a second thread consumes them and computes running
trading indicators (SMA, EMA, VWAP), and a thread-safe queue with a
mutex + condition_variable connects the two safely.

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
