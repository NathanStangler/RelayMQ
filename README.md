# RelayMQ

RelayMQ is a lightweight, low-latency distributed publish-subscribe messaging broker written in C++.

## Build

Requires GCC 14+ and CMake 3.25+:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## Run Tests

```bash
ctest --test-dir build --output-on-failure
```

## Run Server

```bash
# Standalone broker
./build/relay-server --port 9000 --auth-token secret123 --token-rule "token:service.message.>:service.echo.>"

# Clustered brokers
./build/relay-server --port 9001 --cluster-port 9101 --node-id node_1 --cluster-token cluster_secret
./build/relay-server --port 9002 --cluster-port 9102 --node-id node_2 --peers 127.0.0.1:9101 --cluster-token cluster_secret
```

## CLI & Client SDKs

- **Command Line Interface**: [relaymq-cli](https://github.com/NathanStangler/relaymq-cli)
- **C++ Client SDK**: [relaymq-cpp](https://github.com/NathanStangler/relaymq-cpp)
- **Go Client SDK**: [relaymq-go](https://github.com/NathanStangler/relaymq-go)
