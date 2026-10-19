#pragma once

#include "relaymq/broker.hpp"
#include <atomic>
#include <thread>
#include <unordered_map>
#include <memory>
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
    int epoll_fd_;
    int stop_event_fd_;
    uint16_t port_;
    std::atomic<bool> is_running_;
    Broker broker_;
    std::thread event_thread_;
    std::unordered_map<int, std::shared_ptr<Connection>> clients_;
    std::atomic<uint64_t> next_client_id_;

    void event_loop();
    void accept_connection();
    void handle_client(int client_fd, uint32_t events);
    void remove_client(int client_fd);
};

}
