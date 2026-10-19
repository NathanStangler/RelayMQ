#include "relaymq/client.hpp"

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <chrono>

namespace relaymq {

Client::Client() : socket_fd_(-1), is_connected_(false), next_sid_(1), awaiting_pong_(false) {}

Client::~Client() {
    disconnect();
}

bool Client::connect(const std::string& host, uint16_t port) {
    if (is_connected_) {
        return true;
    }

    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        return false;
    }

    int nodelay = 1;
    setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    if (::connect(socket_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    is_connected_ = true;
    receive_thread_ = std::thread(&Client::receive_loop, this);
    return true;
}

void Client::disconnect() {
    is_connected_ = false;

    if (socket_fd_ >= 0) {
        shutdown(socket_fd_, SHUT_RDWR);
        close(socket_fd_);
        socket_fd_ = -1;
    }

    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }

    pong_cv_.notify_all();
}

bool Client::is_connected() const {
    return is_connected_;
}

void Client::set_disconnect_callback(DisconnectCallback callback) {
    std::unique_lock<std::mutex> lock(sub_mutex_);
    disconnect_callback_ = std::move(callback);
}

bool Client::send_raw(const std::string& data) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (!is_connected_ || socket_fd_ < 0) {
        return false;
    }

    size_t total_sent = 0;
    const char* buffer = data.data();
    size_t length = data.size();

    while (total_sent < length) {
        ssize_t sent = send(socket_fd_, buffer + total_sent, length - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            is_connected_ = false;
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return true;
}

bool Client::publish(const std::string& subject, const std::string& payload, const std::string& reply_to) {
    std::string msg = ProtocolParser::format_pub(subject, payload, reply_to);
    return send_raw(msg);
}

std::string Client::subscribe(const std::string& subject, MessageCallback callback) {
    uint64_t sid_num = next_sid_++;
    std::string sid = std::to_string(sid_num);

    std::unique_lock<std::mutex> lock(sub_mutex_);
    subscriptions_[sid] = std::move(callback);
    lock.unlock();

    std::string msg = ProtocolParser::format_sub(subject, sid);
    if (!send_raw(msg)) {
        lock.lock();
        subscriptions_.erase(sid);
        lock.unlock();
        return "";
    }

    return sid;
}

bool Client::unsubscribe(const std::string& sid) {
    std::unique_lock<std::mutex> lock(sub_mutex_);
    subscriptions_.erase(sid);
    lock.unlock();

    std::string msg = ProtocolParser::format_unsub(sid);
    return send_raw(msg);
}

int64_t Client::ping(int timeout_ms) {
    if (!is_connected_) {
        return -1;
    }

    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
    awaiting_pong_ = true;

    if (!send_raw(ProtocolParser::format_ping())) {
        awaiting_pong_ = false;
        return -1;
    }

    std::unique_lock<std::mutex> lock(pong_mutex_);
    bool received = pong_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this]() {
        return !awaiting_pong_.load() || !is_connected_.load();
    });

    if (!received || !is_connected_) {
        awaiting_pong_ = false;
        return -1;
    }

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
}

void Client::receive_loop() {
    char buffer[4096];

    while (is_connected_) {
        ssize_t bytes_read = recv(socket_fd_, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            is_connected_ = false;
            DisconnectCallback cb;
            std::unique_lock<std::mutex> lock(sub_mutex_);
            cb = disconnect_callback_;
            lock.unlock();

            if (cb) {
                cb();
            }
            break;
        }

        parser_.append(buffer, static_cast<size_t>(bytes_read));

        Command cmd;
        while (parser_.next_command(cmd)) {
            if (cmd.type == CommandType::MSG) {
                MessageCallback cb;
                std::unique_lock<std::mutex> lock(sub_mutex_);
                auto it = subscriptions_.find(cmd.sid);
                if (it != subscriptions_.end()) {
                    cb = it->second;
                }
                lock.unlock();

                if (cb) {
                    cb(Message{cmd.subject, cmd.sid, cmd.reply_to, cmd.payload});
                }
            } else if (cmd.type == CommandType::PONG) {
                std::unique_lock<std::mutex> lock(pong_mutex_);
                awaiting_pong_ = false;
                lock.unlock();

                pong_cv_.notify_all();
            }
        }
    }

    pong_cv_.notify_all();
}

}
