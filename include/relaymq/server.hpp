#pragma once

#include "relaymq/broker.hpp"
#include "relaymq/spsc_queue.hpp"
#include <atomic>
#include <thread>
#include <unordered_map>
#include <memory>
#include <string>
#include <cstdint>

namespace relaymq {

struct OutboundMessage {
    std::shared_ptr<Connection> conn;
    std::string data;
};

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
    std::thread writer_thread_;
    SpscQueue<OutboundMessage, 65536> outbound_queue_;
    std::unordered_map<int, std::shared_ptr<Connection>> clients_;
    std::atomic<uint64_t> next_client_id_;

    void event_loop();
    void writer_loop();
    void flush_batch(int fd, std::string& batch);
    void accept_connection();
    void handle_client(int client_fd, uint32_t events);
    void remove_client(int client_fd);
};

}
