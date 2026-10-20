#include "relaymq/server.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <csignal>
#include <exception>

static void print_usage() {
    std::cout << "RelayMQ Server\n\n"
              << "Usage:\n"
              << "  relay-server [options]\n"
              << "  relay-server [port]\n"
              << "  relay-server --help\n\n"
              << "Options:\n"
              << "  --port <port>            Client TCP port (default: 9000)\n"
              << "  --cluster-port <port>    Cluster peering port (default: 0 / disabled)\n"
              << "  --cluster-name <name>    Cluster name (default: default)\n"
              << "  --node-id <id>           Unique node ID (default: auto)\n"
              << "  --peers <h:p,h:p,...>    Comma-separated list of peer addresses\n"
              << "  --heartbeat <ms>         Heartbeat interval in ms (default: 1000)\n"
              << "  --max-missed <count>     Max missed heartbeats before peer drop (default: 3)\n"
              << "  --help                   Show this help message\n";
}

static std::vector<std::string> split_comma(const std::string& str) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t end = str.find(',');
    while (end != std::string::npos) {
        if (end > start) {
            result.push_back(str.substr(start, end - start));
        }
        start = end + 1;
        end = str.find(',', start);
    }
    if (start < str.size()) {
        result.push_back(str.substr(start));
    }
    return result;
}

int main(int argc, char* argv[]) {
    uint16_t port = 9000;
    uint16_t cluster_port = 0;
    std::string cluster_name = "default";
    std::string node_id;
    std::vector<std::string> peers;
    int heartbeat_ms = 1000;
    int max_missed = 3;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage();
            return 0;
        } else if (arg == "--port" && i + 1 < argc) {
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
        } else if (arg == "--cluster-port" && i + 1 < argc) {
            i++;
            try {
                int p = std::stoi(argv[i]);
                if (p < 0 || p > 65535) {
                    std::cerr << "Invalid cluster-port number: " << argv[i] << "\n";
                    return 1;
                }
                cluster_port = static_cast<uint16_t>(p);
            } catch (const std::exception&) {
                std::cerr << "Invalid cluster-port argument: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg == "--cluster-name" && i + 1 < argc) {
            i++;
            cluster_name = argv[i];
        } else if (arg == "--node-id" && i + 1 < argc) {
            i++;
            node_id = argv[i];
        } else if (arg == "--peers" && i + 1 < argc) {
            i++;
            peers = split_comma(argv[i]);
        } else if (arg == "--heartbeat" && i + 1 < argc) {
            i++;
            try {
                heartbeat_ms = std::stoi(argv[i]);
            } catch (const std::exception&) {
                std::cerr << "Invalid heartbeat argument: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg == "--max-missed" && i + 1 < argc) {
            i++;
            try {
                max_missed = std::stoi(argv[i]);
            } catch (const std::exception&) {
                std::cerr << "Invalid max-missed argument: " << argv[i] << "\n";
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

    relaymq::ServerConfig config;
    config.port = port;
    config.enable_clustering = (cluster_port > 0 || !peers.empty());
    config.cluster_port = cluster_port;
    config.cluster_name = cluster_name;
    if (node_id.empty()) {
        node_id = "node_" + std::to_string(cluster_port > 0 ? cluster_port : port);
    }
    config.node_id = node_id;
    config.peers = peers;
    config.heartbeat_interval_ms = heartbeat_ms;
    config.max_missed_heartbeats = max_missed;

    relaymq::Server server;
    if (!server.start(config)) {
        std::cerr << "[RelayMQ Server] Failed to start server on port " << port << "\n";
        return 1;
    }

    std::cout << "[RelayMQ Server] Client port: " << server.port() << "\n";
    if (server.cluster_port() > 0) {
        std::cout << "[RelayMQ Server] Cluster: " << server.cluster_name()
                  << " | Node ID: " << server.node_id()
                  << " | Cluster port: " << server.cluster_port() << "\n";
        if (!peers.empty()) {
            std::cout << "[RelayMQ Server] Peers: " << argv[argc - 1] << "\n";
        }
    }
    std::cout << "[RelayMQ Server] Ready. Press Ctrl+C to shut down.\n";

    int received_signal = 0;
    sigwait(&sigset, &received_signal);

    std::cout << "\n[RelayMQ Server] Shutting down...\n";
    server.stop();
    std::cout << "[RelayMQ Server] Stopped.\n";
    return 0;
}
