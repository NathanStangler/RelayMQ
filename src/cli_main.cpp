#include "relaymq/client.hpp"
#include "relaymq/endpoint.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <csignal>
#include <mutex>
#include <condition_variable>
#include <thread>

struct CliArgs {
    std::string mode;
    std::string subject;
    std::string payload;
    std::string host = "127.0.0.1";
    uint16_t port = 9000;
    std::string token;
};

static void print_usage() {
    std::cout << "RelayMQ CLI\n\n"
              << "Usage:\n"
              << "  relay-cli pub <subject> <payload> [host:port] [--token <tok>]   Publish a message\n"
              << "  relay-cli sub <subject> [host:port] [--token <tok>]             Subscribe and listen for messages\n"
              << "  relay-cli req <subject> <payload> [host:port] [--token <tok>]   Request-reply\n"
              << "  relay-cli ping [host:port] [--token <tok>]                      Ping the broker and measure latency\n\n"
              << "Defaults: host = 127.0.0.1, port = 9000\n";
}

static bool parse_cli_args(int argc, char* argv[], CliArgs& args) {
    if (argc < 2) {
        return false;
    }

    std::vector<std::string> command_args;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage();
            return false;
        }
        if (arg == "--token" && i + 1 < argc) {
            i++;
            args.token = argv[i];
            continue;
        }
        command_args.push_back(std::move(arg));
    }

    if (command_args.empty()) {
        return false;
    }

    args.mode = command_args[0];

    if (args.mode == "pub") {
        if (command_args.size() < 3) {
            std::cerr << "Error: pub requires <subject> and <payload>\n";
            return false;
        }
        args.subject = command_args[1];
        args.payload = command_args[2];
        if (command_args.size() >= 4) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[3], endpoint)) {
                args.host = endpoint.host;
                args.port = endpoint.port;
            }
        }
        if (command_args.size() >= 5) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[4], endpoint)) {
                args.port = endpoint.port;
            }
        }
        return true;
    }

    if (args.mode == "sub") {
        if (command_args.size() < 2) {
            std::cerr << "Error: sub requires <subject>\n";
            return false;
        }
        args.subject = command_args[1];
        if (command_args.size() >= 3) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[2], endpoint)) {
                args.host = endpoint.host;
                args.port = endpoint.port;
            }
        }
        if (command_args.size() >= 4) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[3], endpoint)) {
                args.port = endpoint.port;
            }
        }
        return true;
    }

    if (args.mode == "req") {
        if (command_args.size() < 3) {
            std::cerr << "Error: req requires <subject> and <payload>\n";
            return false;
        }
        args.subject = command_args[1];
        args.payload = command_args[2];
        if (command_args.size() >= 4) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[3], endpoint)) {
                args.host = endpoint.host;
                args.port = endpoint.port;
            }
        }
        if (command_args.size() >= 5) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[4], endpoint)) {
                args.port = endpoint.port;
            }
        }
        return true;
    }

    if (args.mode == "ping") {
        if (command_args.size() >= 2) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[1], endpoint)) {
                args.host = endpoint.host;
                args.port = endpoint.port;
            }
        }
        if (command_args.size() >= 3) {
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(command_args[2], endpoint)) {
                args.port = endpoint.port;
            }
        }
        return true;
    }

    std::cerr << "Unknown command: " << args.mode << "\n";
    return false;
}

static int handle_pub(const CliArgs& args) {
    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(args.host, args.port, args.token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << args.host << ":" << args.port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    if (client.publish(args.subject, args.payload)) {
        std::cout << "[PUB] Successfully published to '" << args.subject << "': " << args.payload << "\n";
    } else {
        std::cerr << "Failed to publish message\n";
        return 1;
    }
    return 0;
}

static int handle_sub(const CliArgs& args) {
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "\n[ERROR] " << err << "\n";
    });

    if (!client.connect(args.host, args.port, args.token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << args.host << ":" << args.port;
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

    std::string sid = client.subscribe(args.subject, [](const relaymq::Message& msg) {
        std::cout << "[" << msg.subject << "] " << msg.payload << "\n";
    });

    if (sid.empty()) {
        std::cerr << "Failed to subscribe to '" << args.subject << "'\n";
        return 1;
    }

    std::cout << "[SUB] Subscribed to '" << args.subject << "' (sid=" << sid << ") on " << args.host << ":" << args.port << "\n";
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

static int handle_req(const CliArgs& args) {
    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(args.host, args.port, args.token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << args.host << ":" << args.port;
        if (!client.last_error().empty()) {
            std::cerr << " (" << client.last_error() << ")";
        }
        std::cerr << "\n";
        return 1;
    }

    std::optional<relaymq::Message> reply = client.request(args.subject, args.payload, 3000);
    if (reply.has_value()) {
        std::cout << "[REPLY] " << reply->payload << "\n";
        client.disconnect();
        return 0;
    }

    std::cerr << "Request timed out or no response\n";
    client.disconnect();
    return 1;
}

static int handle_ping(const CliArgs& args) {
    relaymq::Client client;
    client.set_error_callback([](const std::string& err) {
        std::cerr << "[ERROR] " << err << "\n";
    });

    if (!client.connect(args.host, args.port, args.token)) {
        std::cerr << "Failed to connect to RelayMQ server at " << args.host << ":" << args.port;
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
    CliArgs args;
    if (!parse_cli_args(argc, argv, args)) {
        print_usage();
        return 1;
    }

    if (args.mode == "pub") {
        return handle_pub(args);
    }
    if (args.mode == "sub") {
        return handle_sub(args);
    }
    if (args.mode == "req") {
        return handle_req(args);
    }
    if (args.mode == "ping") {
        return handle_ping(args);
    }

    return 1;
}
