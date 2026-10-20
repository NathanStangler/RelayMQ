#include "relaymq/server.hpp"
#include <iostream>
#include <string>
#include <csignal>
#include <exception>

static void print_usage() {
    std::cout << "RelayMQ Server\n\n"
              << "Usage:\n"
              << "  relay-server [--port <port>]\n"
              << "  relay-server [port]\n"
              << "  relay-server -h | --help\n\n"
              << "Options:\n"
              << "  --port <port>   Port number to listen on (default: 9000)\n"
              << "  --help          Show this help message\n";
}

int main(int argc, char* argv[]) {
    uint16_t port = 9000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage();
            return 0;
        } else if ((arg == "--port") && i + 1 < argc) {
            i++;
            try {
                int p = std::stoi(argv[i]);
                if (p <= 0 || p > 65535) {
                    std::cerr << "Invalid port number: " << argv[i] << " (must be 1-65535)\n";
                    return 1;
                }
                port = static_cast<uint16_t>(p);
            } catch (const std::exception&) {
                std::cerr << "Invalid port argument: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg.rfind("--port=", 0) == 0) {
            std::string value = arg.substr(7);
            try {
                int p = std::stoi(value);
                if (p <= 0 || p > 65535) {
                    std::cerr << "Invalid port number: " << value << " (must be 1-65535)\n";
                    return 1;
                }
                port = static_cast<uint16_t>(p);
            } catch (const std::exception&) {
                std::cerr << "Invalid port argument: " << value << "\n";
                return 1;
            }
        } else {
            try {
                int p = std::stoi(arg);
                if (p <= 0 || p > 65535) {
                    std::cerr << "Invalid port number: " << arg << " (must be 1-65535)\n";
                    return 1;
                }
                port = static_cast<uint16_t>(p);
            } catch (const std::exception&) {
                std::cerr << "Unknown option: " << arg << "\n";
                print_usage();
                return 1;
            }
        }
    }

    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = port;
    if (!server.start(config)) {
        std::cerr << "[RelayMQ Server] Failed to bind to port " << port << "\n";
        return 1;
    }

    std::cout << "[RelayMQ Server] Listening on port " << port << "\n";
    std::cout << "[RelayMQ Server] Press Ctrl+C to shut down.\n";

    int received_signal = 0;
    sigwait(&sigset, &received_signal);

    std::cout << "\n[RelayMQ Server] Shutting down...\n";
    server.stop();
    std::cout << "[RelayMQ Server] Stopped.\n";
    return 0;
}
