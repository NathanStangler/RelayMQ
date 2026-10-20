#pragma once

#include "relaymq/protocol.hpp"
#include <string>
#include <string_view>
#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>

namespace relaymq {

class Peer {
public:
    Peer(int socket_fd, const std::string& node_id, const std::string& cluster_name);
    ~Peer();

    Peer(const Peer&) = delete;
    Peer& operator=(const Peer&) = delete;

    int socket_fd() const;
    std::string node_id();
    void set_node_id(const std::string& id);
    std::string cluster_name();
    bool is_connected() const;
    bool is_handshake_complete() const;
    void set_handshake_complete(bool complete);
    ProtocolParser& parser();
    bool send(std::string_view data);
    void close_connection();
    void record_activity();
    int increment_unanswered_pings();
    void reset_unanswered_pings();
    int unanswered_pings() const;
    void set_endpoint(const std::string& host, uint16_t port);
    std::string endpoint();
    std::chrono::steady_clock::time_point last_activity();

private:
    std::atomic<int> socket_fd_;
    std::mutex peer_mutex_;
    std::string node_id_;
    std::string cluster_name_;
    std::atomic<bool> is_connected_;
    std::atomic<bool> handshake_complete_;
    std::atomic<int> unanswered_pings_{0};
    std::string endpoint_;
    std::chrono::steady_clock::time_point last_activity_{std::chrono::steady_clock::now()};
    std::mutex send_mutex_;
    ProtocolParser parser_;
};

}
