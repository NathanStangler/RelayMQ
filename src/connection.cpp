#include "relaymq/connection.hpp"
#include "relaymq/net.hpp"

namespace relaymq {

Connection::Connection(int socket_fd, const std::string& client_id, bool authenticated)
    : socket_fd_(socket_fd), client_id_(client_id), is_open_(true), is_authenticated_(authenticated), connected_at_(std::chrono::steady_clock::now()) {}

Connection::~Connection() {
    close_connection();
}

const std::string& Connection::id() const {
    return client_id_;
}

int Connection::socket_fd() const {
    return socket_fd_.load(std::memory_order_relaxed);
}

bool Connection::is_open() const {
    return is_open_.load(std::memory_order_acquire);
}

bool Connection::is_authenticated() const {
    return is_authenticated_.load(std::memory_order_acquire);
}

void Connection::set_authenticated(bool authenticated) {
    is_authenticated_.store(authenticated, std::memory_order_release);
}

std::chrono::steady_clock::time_point Connection::connected_at() const {
    return connected_at_;
}

ProtocolParser& Connection::parser() {
    return parser_;
}

void Connection::close_connection() {
    bool expected = true;
    if (is_open_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        int fd = socket_fd_.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            net::close_socket(fd);
        }
    }
}

}
