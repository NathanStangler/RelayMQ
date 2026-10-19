#pragma once

#include "relaymq/protocol.hpp"
#include <string>
#include <memory>
#include <atomic>

namespace relaymq {

class Connection {
public:
    Connection(int socket_fd, const std::string& client_id);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    const std::string& id() const;
    int socket_fd() const;
    bool is_open() const;
    ProtocolParser& parser();
    void close_connection();

private:
    std::atomic<int> socket_fd_;
    std::string client_id_;
    ProtocolParser parser_;
    std::atomic<bool> is_open_;
};

}
