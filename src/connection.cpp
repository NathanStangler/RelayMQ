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

void Connection::close_connection() {
    if (is_open_) {
        is_open_ = false;
        if (socket_fd_ >= 0) {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
}

}
