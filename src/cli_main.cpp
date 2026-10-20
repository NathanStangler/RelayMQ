#include "relaymq/client.hpp"
#include <iostream>
#include <string>
#include <csignal>
#include <cctype>
#include <mutex>
#include <condition_variable>
#include <thread>

static void parse_endpoint(int argc, char* argv[], int start_idx, std::string& host, uint16_t& port) {
    host = "127.0.0.1";
    port = 9000;

    if (start_idx >= argc) {
        return;
    }

    std::string first = argv[start_idx];
    size_t colon_pos = first.find(':');
    if (colon_pos != std::string::npos) {
        host = first.substr(0, colon_pos);
        try {
            int p = std::stoi(first.substr(colon_pos + 1));
            if (p > 0 && p <= 65535) {
                port = static_cast<uint16_t>(p);
            }
        } catch (const std::exception&) {}
        return;
    }

    bool is_number = !first.empty();
    for (size_t i = 0; i < first.size(); i++) {
        if (!std::isdigit(static_cast<unsigned char>(first[i]))) {
            is_number = false;
            break;
        }
    }

    if (is_number) {
        try {
            int p = std::stoi(first);
            if (p > 0 && p <= 65535) {
                port = static_cast<uint16_t>(p);
            }
        } catch (const std::exception&) {}
    } else {
        host = first;
        if (start_idx + 1 < argc) {
            try {
                int p = std::stoi(argv[start_idx + 1]);
                if (p > 0 && p <= 65535) {
                    port = static_cast<uint16_t>(p);
                }
            } catch (const std::exception&) {}
        }
    }
}

static std::string extract_token(int& argc, char* argv[]) {
    std::string token;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--token" && i + 1 < argc) {
            token = argv[i + 1];
            for (int j = i; j + 2 < argc; j++) {
                argv[j] = argv[j + 2];
            }
            argc -= 2;
            i--;
        }
    }
    return token;
}

static void print_usage() {
    std::cout << "RelayMQ CLI\n\n"
              << "Usage:\n"
              << "  relay-cli pub <subject> <payload> [host] [port] [--token <tok>]   Publish a message\n"
              << "  relay-cli sub <subject> [host] [port] [--token <tok>]             Subscribe and listen for messages\n"
              << "  relay-cli req <subject> <payload> [host] [port] [--token <tok>]   Request-reply\n"
              << "  relay-cli ping [host] [port] [--token <tok>]                      Ping the broker and measure latency\n\n"
              << "Defaults: host = 127.0.0.1, port = 9000\n";
}

static int handle_pub(int argc, char* argv[], const std::string& token) {
    if (argc < 4) {
        std::cerr << "Error: pub requires <subject> and <payload>\n";
        std::cerr << "Usage: relay-cli pub <subject> <payload> [host] [port] [--token <tok>]\n";
        return 1;
    }

    std::string subject = argv[2];
    std::string payload = argv[3];
    std::string host;
    uint16_t port = 9000;
    parse_endpoint(argc, argv, 4, host, port);

    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(host, port, token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << host << ":" << port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    if (client.publish(subject, payload)) {
        std::cout << "[PUB] Successfully published to '" << subject << "': " << payload << "\n";
    } else {
        std::cerr << "Failed to publish message\n";
        return 1;
    }
    return 0;
}

static int handle_sub(int argc, char* argv[], const std::string& token) {
    if (argc < 3) {
        std::cerr << "Error: sub requires <subject>\n";
        std::cerr << "Usage: relay-cli sub <subject> [host] [port] [--token <tok>]\n";
        return 1;
    }

    std::string subject = argv[2];
    std::string host;
    uint16_t port = 9000;
    parse_endpoint(argc, argv, 3, host, port);

    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "\n[ERROR] " << err << "\n";
    });

    if (!client.connect(host, port, token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << host << ":" << port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    std::mutex shutdown_mutex;
    std::condition_variable shutdown_cv;
    bool should_exit = false;

    client.set_disconnect_callback([&]() {
        std::unique_lock<std::mutex> lock(shutdown_mutex);
        should_exit = true;
        lock.unlock();
        shutdown_cv.notify_one();
    });

    std::thread sig_thread([&sigset, &shutdown_mutex, &shutdown_cv, &should_exit]() {
        int sig = 0;
        sigwait(&sigset, &sig);
        std::unique_lock<std::mutex> lock(shutdown_mutex);
        should_exit = true;
        lock.unlock();
        shutdown_cv.notify_one();
    });
    sig_thread.detach();

    std::string sid = client.subscribe(subject, [](const relaymq::Message& msg) {
        std::cout << "[" << msg.subject << "] " << msg.payload << "\n";
    });

    if (sid.empty()) {
        std::cerr << "Failed to subscribe to '" << subject << "'\n";
        return 1;
    }

    std::cout << "[SUB] Subscribed to '" << subject << "' (sid=" << sid << ") on " << host << ":" << port << "\n";
    std::cout << "[SUB] Waiting for messages (Ctrl+C to quit)...\n";

    std::unique_lock<std::mutex> lock(shutdown_mutex);
    shutdown_cv.wait(lock, [&should_exit]() {
        return should_exit;
    });
    lock.unlock();

    if (!client.is_connected()) {
        std::cout << "\n[SUB] Server connection lost.\n";
    } else {
        std::cout << "\n[SUB] Unsubscribing and exiting...\n";
        client.unsubscribe(sid);
    }

    client.disconnect();
    return 0;
}

static int handle_req(int argc, char* argv[], const std::string& token) {
    if (argc < 4) {
        std::cerr << "Error: req requires <subject> and <payload>\n";
        std::cerr << "Usage: relay-cli req <subject> <payload> [host] [port] [--token <tok>]\n";
        return 1;
    }

    std::string subject = argv[2];
    std::string payload = argv[3];
    std::string host;
    uint16_t port = 9000;
    parse_endpoint(argc, argv, 4, host, port);

    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(host, port, token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << host << ":" << port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    std::optional<relaymq::Message> reply = client.request(subject, payload, 3000);
    if (reply.has_value()) {
        std::cout << "[REPLY] " << reply->payload << "\n";
        client.disconnect();
        return 0;
    }

    std::cerr << "Request timed out or no response\n";
    client.disconnect();
    return 1;
}

static int handle_ping(int argc, char* argv[], const std::string& token) {
    std::string host;
    uint16_t port = 9000;
    parse_endpoint(argc, argv, 2, host, port);

    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(host, port, token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << host << ":" << port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    int64_t latency = client.ping();
    if (latency >= 0) {
        std::cout << "[PONG] Latency: " << (static_cast<double>(latency) / 1000.0) << " ms\n";
    } else {
        std::cerr << "Ping timed out or failed\n";
        return 1;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string token = extract_token(argc, argv);
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string mode = argv[1];

    if (mode == "pub") {
        return handle_pub(argc, argv, token);
    }
    if (mode == "sub") {
        return handle_sub(argc, argv, token);
    }
    if (mode == "req") {
        return handle_req(argc, argv, token);
    }
    if (mode == "ping") {
        return handle_ping(argc, argv, token);
    }

    std::cerr << "Unknown command: " << mode << "\n\n";
    print_usage();
    return 1;
}
