#include "relaymq/peer.hpp"

#include <unistd.h>
#include <sys/socket.h>
#include <cerrno>
#include <thread>

namespace relaymq {

Peer::Peer(int socket_fd, const std::string& node_id, const std::string& cluster_name)
    : socket_fd_(socket_fd), node_id_(node_id), cluster_name_(cluster_name), is_connected_(true), handshake_complete_(false) {}

Peer::~Peer() {
    close_connection();
}

int Peer::socket_fd() const {
    return socket_fd_.load(std::memory_order_relaxed);
}

std::string Peer::node_id() {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    std::string id = node_id_;
    lock.unlock();
    return id;
}

void Peer::set_node_id(const std::string& id) {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    node_id_ = id;
    lock.unlock();
}

std::string Peer::cluster_name() {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    std::string name = cluster_name_;
    lock.unlock();
    return name;
}

bool Peer::is_connected() const {
    return is_connected_.load(std::memory_order_acquire);
}

bool Peer::is_handshake_complete() const {
    return handshake_complete_.load(std::memory_order_acquire);
}

void Peer::set_handshake_complete(bool complete) {
    handshake_complete_.store(complete, std::memory_order_release);
}

ProtocolParser& Peer::parser() {
    return parser_;
}

bool Peer::send(std::string_view data) {
    std::unique_lock<std::mutex> lock(send_mutex_);
    int fd = socket_fd_.load(std::memory_order_relaxed);
    if (!is_connected_.load(std::memory_order_acquire) || fd < 0) {
        lock.unlock();
        return false;
    }

    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t sent = ::send(fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::yield();
                continue;
            }
            is_connected_.store(false, std::memory_order_release);
            lock.unlock();
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    lock.unlock();
    return true;
}

void Peer::close_connection() {
    bool expected = true;
    if (is_connected_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        int fd = socket_fd_.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            close(fd);
        }
    }
}

void Peer::record_activity() {
    unanswered_pings_.store(0, std::memory_order_release);
    std::unique_lock<std::mutex> lock(peer_mutex_);
    last_activity_ = std::chrono::steady_clock::now();
    lock.unlock();
}

int Peer::increment_unanswered_pings() {
    return ++unanswered_pings_;
}

void Peer::reset_unanswered_pings() {
    unanswered_pings_.store(0, std::memory_order_release);
}

int Peer::unanswered_pings() const {
    return unanswered_pings_.load(std::memory_order_acquire);
}

void Peer::set_endpoint(const std::string& host, uint16_t port) {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    endpoint_ = host + ":" + std::to_string(port);
    lock.unlock();
}

std::string Peer::endpoint() {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    std::string endpoint = endpoint_;
    lock.unlock();
    return endpoint;
}

std::chrono::steady_clock::time_point Peer::last_activity() {
    std::unique_lock<std::mutex> lock(peer_mutex_);
    std::chrono::steady_clock::time_point last_activity = last_activity_;
    lock.unlock();
    return last_activity;
}

}
