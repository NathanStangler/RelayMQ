#pragma once

#include "relaymq/protocol.hpp"
#include <string>
#include <memory>
#include <atomic>
#include <chrono>

namespace relaymq {

class Connection {
public:
    Connection(int socket_fd, const std::string& client_id, bool authenticated = true);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    const std::string& id() const;
    int socket_fd() const;
    bool is_open() const;
    bool is_authenticated() const;
    void set_authenticated(bool authenticated);
    std::chrono::steady_clock::time_point connected_at() const;
    ProtocolParser& parser();
    void close_connection();

private:
    std::atomic<int> socket_fd_;
    std::string client_id_;
    ProtocolParser parser_;
    std::atomic<bool> is_open_;
    std::atomic<bool> is_authenticated_;
    std::chrono::steady_clock::time_point connected_at_;
};

}
