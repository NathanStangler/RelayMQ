cmake_minimum_required(VERSION 3.25)
project(RelayMQ
    VERSION 1.0.0
    DESCRIPTION "A publish-subscribe message broker system"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
    -Wnon-virtual-dtor
)

find_package(Threads REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(relaymq_core
    src/protocol.cpp
    src/subscription_registry.cpp
    src/connection.cpp
    src/broker.cpp
    src/server.cpp
    src/client.cpp
)
target_include_directories(relaymq_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(relaymq_core PUBLIC
    Threads::Threads
)

add_executable(relay-server
    src/server_main.cpp
)
target_link_libraries(relay-server PRIVATE
    relaymq_core
)

add_executable(relay-cli
    src/cli_main.cpp
)
target_link_libraries(relay-cli PRIVATE
    relaymq_core
)

add_executable(relay-bench
    benchmarks/bench_main.cpp
)
target_link_libraries(relay-bench PRIVATE
    relaymq_core
)

enable_testing()
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

add_executable(relaymq_tests
    tests/test_protocol.cpp
    tests/test_subscription_registry.cpp
    tests/test_server.cpp
    tests/test_spsc_queue.cpp
    tests/test_client.cpp
)
target_link_libraries(relaymq_tests PRIVATE
    relaymq_core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(relaymq_tests)
