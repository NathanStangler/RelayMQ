#pragma once

#include "relaymq/broker.hpp"
#include <atomic>
#include <thread>
#include <vector>
#include <cstdint>

namespace relaymq {

class Server {
public:
    Server();
    ~Server();

    bool start(uint16_t port);
    void stop();
    bool is_running() const;
    uint16_t port() const;
    Broker& broker();

private:
    int server_fd_;
    uint16_t port_;
    std::atomic<bool> is_running_;
    Broker broker_;
    std::thread accept_thread_;
    std::vector<std::thread> client_threads_;
    std::mutex threads_mutex_;
    std::atomic<uint64_t> next_client_id_;

    void accept_loop();
    void handle_client(std::shared_ptr<Connection> conn);
};

}
