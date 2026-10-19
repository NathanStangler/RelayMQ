#include "relaymq/server.hpp"
#include <iostream>
#include <csignal>

int main(int argc, char* argv[]) {
    uint16_t port = 9000;
    if (argc > 1) {
        port = static_cast<uint16_t>(std::stoi(argv[1]));
    }

    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    relaymq::Server server;
    if (!server.start(port)) {
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
