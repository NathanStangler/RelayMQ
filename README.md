# RelayMQ

RelayMQ is a lightweight, low-latency distributed publish-subscribe messaging broker and client library written in C++.

---

### Build

Requires GCC 14+ and CMake 3.25+:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### Run Tests

```bash
ctest --test-dir build --output-on-failure
```

### Run Benchmarks

```bash
# Single-node benchmark
./build/relay-bench --throughput-count 100000 --payload 64

# Cluster benchmark (2 nodes)
./build/relay-bench --cluster --throughput-count 100000 --latency-count 10000 --payload 64
```

### Run Server & CLI

```bash
# Standalone broker
./build/relay-server --port 9000

# Clustered brokers
./build/relay-server --port 9001 --cluster-port 9101 --node-id node_1
./build/relay-server --port 9002 --cluster-port 9102 --node-id node_2 --peers 127.0.0.1:9101

# Subscribe
./build/relay-cli sub "service.*" 127.0.0.1 9000

# Publish
./build/relay-cli pub "service.message" "value=50" 127.0.0.1 9000

# Request-Reply RPC
./build/relay-cli req "service.echo" "ping" 127.0.0.1 9000
```

---

## C++ Client Example

```cpp
#include "relaymq/client.hpp"
#include <iostream>

int main() {
    relaymq::Client client;
    client.connect("127.0.0.1", 9000);

    client.subscribe("service.>", [&client](const relaymq::Message& msg) {
        std::cout << "[" << msg.subject << "] " << msg.payload << "\n";
        if (!msg.reply_to.empty()) {
            client.publish(msg.reply_to, "Received: " + msg.payload);
        }
    });

    client.publish("service.message", "UPDATE VALUE");

    std::optional<relaymq::Message> reply = client.request("service.update", "VALUE", 500);
    if (reply) {
        std::cout << "Reply: " << reply->payload << "\n";
    }

    client.disconnect();
    return 0;
}
```
