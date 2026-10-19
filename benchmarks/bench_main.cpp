#include "relaymq/server.hpp"
#include "relaymq/client.hpp"

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstdint>

struct BenchConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 0;
    size_t throughput_count = 50000;
    size_t latency_count = 5000;
    size_t payload_size = 64;
    bool run_throughput = true;
    bool run_latency = true;
};

struct LatencyStats {
    double min_us = 0.0;
    double mean_us = 0.0;
    double p50_us = 0.0;
    double p90_us = 0.0;
    double p99_us = 0.0;
    double p99_9_us = 0.0;
    double max_us = 0.0;
};

static LatencyStats calculate_stats(std::vector<int64_t>& samples_ns) {
    LatencyStats stats{};
    if (samples_ns.empty()) {
        return stats;
    }

    std::sort(samples_ns.begin(), samples_ns.end());
    size_t n = samples_ns.size();

    int64_t sum_ns = 0;
    for (size_t i = 0; i < n; i++) {
        sum_ns += samples_ns[i];
    }

    stats.min_us = static_cast<double>(samples_ns.front()) / 1000.0;
    stats.max_us = static_cast<double>(samples_ns.back()) / 1000.0;
    stats.mean_us = (static_cast<double>(sum_ns) / static_cast<double>(n)) / 1000.0;
    stats.p50_us = static_cast<double>(samples_ns[n * 50 / 100]) / 1000.0;
    stats.p90_us = static_cast<double>(samples_ns[n * 90 / 100]) / 1000.0;
    stats.p99_us = static_cast<double>(samples_ns[n * 99 / 100]) / 1000.0;
    stats.p99_9_us = static_cast<double>(samples_ns[n * 999 / 1000]) / 1000.0;

    return stats;
}

static void print_latency_table(const std::string& title, const LatencyStats& stats, size_t count) {
    std::cout << "\n--- " << title << " (" << count << " iterations) ---\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << std::left << std::setw(12) << "Metric" << std::right << std::setw(16) << "Latency (us)" << std::setw(16) << "Latency (ms)" << "\n";
    std::cout << std::string(44, '-') << "\n";
    std::cout << std::left << std::setw(12) << "Min" << std::right << std::setw(16) << stats.min_us << std::setw(16) << (stats.min_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "Mean" << std::right << std::setw(16) << stats.mean_us << std::setw(16) << (stats.mean_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p50" << std::right << std::setw(16) << stats.p50_us << std::setw(16) << (stats.p50_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p90" << std::right << std::setw(16) << stats.p90_us << std::setw(16) << (stats.p90_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p99" << std::right << std::setw(16) << stats.p99_us << std::setw(16) << (stats.p99_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p99.9" << std::right << std::setw(16) << stats.p99_9_us << std::setw(16) << (stats.p99_9_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "Max" << std::right << std::setw(16) << stats.max_us << std::setw(16) << (stats.max_us / 1000.0) << "\n";
}

static void run_throughput_benchmark(const std::string& host, uint16_t port, size_t count, size_t payload_size) {
    std::cout << "Throughput Benchmark\n\n";
    std::cout << "Messages: " << count << "\n";
    std::cout << "Payload size: " << payload_size << " bytes\n";

    std::string payload(payload_size, 'X');
    const std::string subject = "service.bench.throughput";

    relaymq::Client sub_client;
    if (!sub_client.connect(host, port)) {
        std::cerr << "Throughput benchmark: failed to connect subscriber.\n";
        return;
    }

    std::mutex cv_mutex;
    std::condition_variable cv;
    size_t received_count = 0;

    sub_client.subscribe(subject, [&](const relaymq::Message&) {
        std::unique_lock<std::mutex> lock(cv_mutex);
        received_count++;
        if (received_count == count) {
            lock.unlock();
            cv.notify_one();
        }
    });

    relaymq::Client pub_client;
    if (!pub_client.connect(host, port)) {
        std::cerr << "Throughput benchmark: failed to connect publisher.\n";
        return;
    }

    for (size_t i = 0; i < 100; i++) {
        pub_client.publish(subject, "warmup");
    }
    std::unique_lock<std::mutex> reset_lock(cv_mutex);
    received_count = 0;
    reset_lock.unlock();

    std::cout << "Publishing " << count << " messages...\n";

    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

    for (size_t i = 0; i < count; i++) {
        pub_client.publish(subject, payload);
    }

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool completed = cv.wait_for(lock, std::chrono::seconds(10), [&]() {
        return received_count >= count;
    });
    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    lock.unlock();

    if (!completed) {
        std::cerr << "Throughput benchmark timed out. Received " << received_count << " / " << count << " messages.\n";
        return;
    }

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double msgs_per_sec = static_cast<double>(count) / elapsed_sec;
    double bytes_sent = static_cast<double>(count * payload_size);
    double mb_per_sec = (bytes_sent / (1024.0 * 1024.0)) / elapsed_sec;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nThroughput Results:\n";
    std::cout << "  Elapsed Time: " << (elapsed_sec * 1000.0) << " ms\n";
    std::cout << "  Throughput: " << std::setprecision(0) << msgs_per_sec << " msgs/sec\n";
    std::cout << "  Bandwidth: " << std::setprecision(2) << mb_per_sec << " MB/sec\n";

    pub_client.disconnect();
    sub_client.disconnect();
}

static void run_ping_latency_benchmark(const std::string& host, uint16_t port, size_t count) {
    relaymq::Client client;
    if (!client.connect(host, port)) {
        std::cerr << "Ping latency benchmark: failed to connect.\n";
        return;
    }

    for (size_t i = 0; i < 50; i++) {
        client.ping();
    }

    std::vector<int64_t> samples_ns;
    samples_ns.reserve(count);

    for (size_t i = 0; i < count; i++) {
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        int64_t response_time = client.ping();
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        if (response_time >= 0) {
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            samples_ns.push_back(ns);
        }
    }

    LatencyStats stats = calculate_stats(samples_ns);
    print_latency_table("Ping RTT", stats, samples_ns.size());

    client.disconnect();
}

static void run_pubsub_latency_benchmark(const std::string& host, uint16_t port, size_t count, size_t payload_size) {
    const std::string req_subject = "service.bench.latency.req";
    const std::string res_subject = "service.bench.latency.res";
    std::string payload(payload_size, 'A');

    relaymq::Client echo_client;
    if (!echo_client.connect(host, port)) {
        std::cerr << "Pub/Sub latency benchmark: failed to connect echo client.\n";
        return;
    }

    echo_client.subscribe(req_subject, [&](const relaymq::Message& msg) {
        echo_client.publish(res_subject, msg.payload);
    });

    relaymq::Client req_client;
    if (!req_client.connect(host, port)) {
        std::cerr << "Pub/Sub latency benchmark: failed to connect requester client.\n";
        return;
    }

    std::mutex mtx;
    std::condition_variable cv;
    bool reply_received = false;

    req_client.subscribe(res_subject, [&](const relaymq::Message&) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = true;
        lock.unlock();
        cv.notify_one();
    });

    for (size_t i = 0; i < 50; i++) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = false;
        req_client.publish(req_subject, "warmup");
        cv.wait_for(lock, std::chrono::milliseconds(100), [&]() {
            return reply_received;
        });
    }

    std::vector<int64_t> samples_ns;
    samples_ns.reserve(count);

    for (size_t i = 0; i < count; i++) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = false;

        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        req_client.publish(req_subject, payload);

        bool ok = cv.wait_for(lock, std::chrono::milliseconds(500), [&]() {
            return reply_received;
        });
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        lock.unlock();

        if (ok) {
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            samples_ns.push_back(ns);
        }
    }

    LatencyStats stats = calculate_stats(samples_ns);
    print_latency_table("Pub/Sub RTT", stats, samples_ns.size());

    req_client.disconnect();
    echo_client.disconnect();
}

static void print_usage() {
    std::cout << "RelayMQ Benchmarking\n\n"
              << "Usage: relay-bench [options]\n\n"
              << "Options:\n"
              << "  --server <host:port>     Connect to a running broker\n"
              << "  --throughput-count <N>   Number of messages for throughput test (default: 50000)\n"
              << "  --latency-count <N>      Number of iterations for latency test (default: 5000)\n"
              << "  --payload <bytes>        Message payload size in bytes (default: 64)\n"
              << "  --throughput-only        Run only the throughput benchmark\n"
              << "  --latency-only           Run only the latency benchmarks\n"
              << "  --help                   Display this help message\n";
}

int main(int argc, char* argv[]) {
    BenchConfig config;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage();
            return 0;
        } else if (arg == "--server" && i + 1 < argc) {
            i++;
            std::string s = argv[i];
            size_t colon = s.find(':');
            if (colon != std::string::npos) {
                config.host = s.substr(0, colon);
                try {
                    config.port = static_cast<uint16_t>(std::stoi(s.substr(colon + 1)));
                } catch (const std::exception&) {
                    std::cerr << "Invalid port in --server argument: " << s << "\n";
                    return 1;
                }
            } else {
                config.host = s;
                config.port = 9000;
            }
        } else if (arg == "--throughput-count" && i + 1 < argc) {
            i++;
            try {
                config.throughput_count = static_cast<size_t>(std::stoull(argv[i]));
            } catch (const std::exception&) {
                std::cerr << "Invalid --throughput-count: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg == "--latency-count" && i + 1 < argc) {
            i++;
            try {
                config.latency_count = static_cast<size_t>(std::stoull(argv[i]));
            } catch (const std::exception&) {
                std::cerr << "Invalid --latency-count: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg == "--payload" && i + 1 < argc) {
            i++;
            try {
                config.payload_size = static_cast<size_t>(std::stoull(argv[i]));
            } catch (const std::exception&) {
                std::cerr << "Invalid --payload: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg == "--throughput-only") {
            config.run_throughput = true;
            config.run_latency = false;
        } else if (arg == "--latency-only") {
            config.run_throughput = false;
            config.run_latency = true;
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage();
            return 1;
        }
    }

    std::unique_ptr<relaymq::Server> server;

    if (config.port == 0) {
        server = std::make_unique<relaymq::Server>();
        if (!server->start(0)) {
            std::cerr << "Failed to start broker.\n";
            return 1;
        }
        config.port = server->port();
        std::cout << "[RelayMQ Benchmark] Started broker on port " << config.port << "\n";
    } else {
        std::cout << "[RelayMQ Benchmark] Connecting to remote broker at " << config.host << ":" << config.port << "\n";
    }

    if (config.run_throughput) {
        run_throughput_benchmark(config.host, config.port, config.throughput_count, config.payload_size);
    }

    if (config.run_latency) {
        std::cout << "Latency Benchmarks\n\n";
        run_ping_latency_benchmark(config.host, config.port, config.latency_count);
        run_pubsub_latency_benchmark(config.host, config.port, config.latency_count, config.payload_size);
    }

    if (server) {
        server->stop();
    }

    std::cout << "\n[RelayMQ Benchmark] Benchmark complete.\n";
    return 0;
}
