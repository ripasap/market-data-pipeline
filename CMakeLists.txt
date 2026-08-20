cmake_minimum_required(VERSION 3.10)
project(MarketDataPipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(market_pipeline src/main.cpp)
target_include_directories(market_pipeline PRIVATE include)
target_link_libraries(market_pipeline PRIVATE Threads::Threads)
target_compile_options(market_pipeline PRIVATE -Wall -Wextra)
