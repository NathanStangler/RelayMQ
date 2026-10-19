#include "relaymq/connection.hpp"

#include <unistd.h>
#include <sys/socket.h>

namespace relaymq {

Connection::Connection(int socket_fd, const std::string& client_id) : socket_fd_(socket_fd), client_id_(client_id), is_open_(true) {}

Connection::~Connection() {
    close_connection();
}

const std::string& Connection::id() const {
    return client_id_;
}

int Connection::socket_fd() const {
    return socket_fd_;
}

bool Connection::is_open() const {
    return is_open_;
}

ProtocolParser& Connection::parser() {
    return parser_;
}

bool Connection::send_data(const std::string& data) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (!is_open_ || socket_fd_ < 0) {
        return false;
    }

    size_t total_sent = 0;
    const char* buffer = data.data();
    size_t length = data.size();

    while (total_sent < length) {
        ssize_t sent = send(socket_fd_, buffer + total_sent, length - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            is_open_ = false;
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }

    return true;
}

void Connection::close_connection() {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (is_open_) {
        is_open_ = false;
        if (socket_fd_ >= 0) {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
}

}
