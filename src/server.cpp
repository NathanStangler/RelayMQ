#include "relaymq/server.hpp"

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>

namespace relaymq {

Server::Server() : server_fd_(-1), port_(0), is_running_(false), next_client_id_(1) {}

Server::~Server() {
    stop();
}

bool Server::start(uint16_t port) {
    if (is_running_) {
        return false;
    }

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        return false;
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }

    if (listen(server_fd_, 128) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }

    socklen_t addr_len = sizeof(server_addr);
    if (getsockname(server_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), &addr_len) == 0) {
        port_ = ntohs(server_addr.sin_port);
    } else {
        port_ = port;
    }

    is_running_ = true;
    accept_thread_ = std::thread(&Server::accept_loop, this);
    return true;
}

void Server::stop() {
    if (!is_running_) {
        return;
    }

    is_running_ = false;

    if (server_fd_ >= 0) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    broker_.close_all_connections();

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    std::lock_guard<std::mutex> lock(threads_mutex_);
    for (size_t i = 0; i < client_threads_.size(); i++) {
        if (client_threads_[i].joinable()) {
            client_threads_[i].join();
        }
    }
    client_threads_.clear();
}

bool Server::is_running() const {
    return is_running_;
}

uint16_t Server::port() const {
    return port_;
}

Broker& Server::broker() {
    return broker_;
}

void Server::accept_loop() {
    while (is_running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (!is_running_) {
                break;
            }
            continue;
        }

        int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        uint64_t cid = next_client_id_++;
        std::string client_id = "client_" + std::to_string(cid);

        std::shared_ptr<Connection> conn = std::make_shared<Connection>(client_fd, client_id);
        broker_.add_connection(conn);

        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.push_back(std::thread(&Server::handle_client, this, conn));
    }
}

void Server::handle_client(std::shared_ptr<Connection> conn) {
    char buffer[4096];

    while (is_running_ && conn->is_open()) {
        ssize_t bytes_read = recv(conn->socket_fd(), buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            break;
        }

        conn->parser().append(buffer, static_cast<size_t>(bytes_read));

        Command cmd;
        while (conn->parser().next_command(cmd)) {
            broker_.handle_command(conn, cmd);
        }
    }

    broker_.remove_connection(conn->id());
    conn->close_connection();
}

}
