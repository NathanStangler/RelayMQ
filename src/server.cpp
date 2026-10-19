#include "relaymq/server.hpp"

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>

namespace relaymq {

Server::Server() : server_fd_(-1), epoll_fd_(-1), stop_event_fd_(-1), port_(0), is_running_(false), next_client_id_(1) {}

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

    int flags = fcntl(server_fd_, F_GETFL, 0);
    fcntl(server_fd_, F_SETFL, flags | O_NONBLOCK);

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

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }

    stop_event_fd_ = eventfd(0, EFD_NONBLOCK);
    if (stop_event_fd_ < 0) {
        close(epoll_fd_);
        close(server_fd_);
        epoll_fd_ = -1;
        server_fd_ = -1;
        return false;
    }

    epoll_event server_ev{};
    server_ev.events = EPOLLIN;
    server_ev.data.fd = server_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &server_ev);

    epoll_event stop_ev{};
    stop_ev.events = EPOLLIN;
    stop_ev.data.fd = stop_event_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, stop_event_fd_, &stop_ev);

    broker_.set_outbound_handler([this](std::shared_ptr<Connection> conn, std::string data) {
        while (is_running_ && !outbound_queue_.push(OutboundMessage{std::move(conn), std::move(data)})) {
            std::this_thread::yield();
        }
    });

    is_running_ = true;
    writer_thread_ = std::thread(&Server::writer_loop, this);
    event_thread_ = std::thread(&Server::event_loop, this);
    return true;
}

void Server::stop() {
    if (!is_running_) {
        return;
    }

    is_running_ = false;

    if (stop_event_fd_ >= 0) {
        uint64_t val = 1;
        ssize_t written = write(stop_event_fd_, &val, sizeof(val));
    }

    if (event_thread_.joinable()) {
        event_thread_.join();
    }

    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (stop_event_fd_ >= 0) {
        close(stop_event_fd_);
        stop_event_fd_ = -1;
    }

    if (server_fd_ >= 0) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    broker_.close_all_connections();
    clients_.clear();
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

void Server::event_loop() {
    epoll_event events[64];

    while (is_running_) {
        int num_ready = epoll_wait(epoll_fd_, events, 64, -1);
        if (num_ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < num_ready; i++) {
            int fd = events[i].data.fd;
            uint32_t ev_flags = events[i].events;

            if (fd == stop_event_fd_) {
                break;
            }

            if (fd == server_fd_) {
                accept_connection();
            } else {
                handle_client(fd, ev_flags);
            }
        }
    }
}

void Server::accept_connection() {
    while (is_running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept4(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        uint64_t cid = next_client_id_++;
        std::string client_id = "client_" + std::to_string(cid);

        std::shared_ptr<Connection> conn = std::make_shared<Connection>(client_fd, client_id);
        broker_.add_connection(conn);
        clients_[client_fd] = conn;

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
    }
}

void Server::handle_client(int client_fd, uint32_t events) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }
    std::shared_ptr<Connection> conn = it->second;

    if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        remove_client(client_fd);
        return;
    }

    char buffer[16384];
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), MSG_DONTWAIT);
    if (bytes_read > 0) {
        conn->parser().append(buffer, static_cast<size_t>(bytes_read));
        Command cmd;
        while (conn->parser().next_command(cmd)) {
            broker_.handle_command(conn, cmd);
        }
    } else if (bytes_read == 0) {
        remove_client(client_fd);
    } else {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            remove_client(client_fd);
        }
    }
}

void Server::remove_client(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it != clients_.end()) {
        std::shared_ptr<Connection> conn = it->second;
        clients_.erase(it);

        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
        broker_.remove_connection(conn->id());
        conn->close_connection();
    }
}

void Server::flush_batch(int fd, std::string& batch) {
    if (fd < 0 || batch.empty()) {
        return;
    }

    size_t total_sent = 0;
    while (total_sent < batch.size()) {
        ssize_t sent = send(fd, batch.data() + total_sent, batch.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::yield();
                continue;
            }
            break;
        }
        total_sent += static_cast<size_t>(sent);
    }
    batch.clear();
}

void Server::writer_loop() {
    std::string batch;
    batch.reserve(65536);
    int current_fd = -1;

    while (is_running_) {
        OutboundMessage msg;
        if (outbound_queue_.pop(msg)) {
            if (!msg.conn || !msg.conn->is_open()) {
                continue;
            }

            int fd = msg.conn->socket_fd();
            if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
                flush_batch(current_fd, batch);
                current_fd = fd;
            }

            batch.append(msg.data);

            while (outbound_queue_.pop(msg)) {
                if (!msg.conn || !msg.conn->is_open()) {
                    continue;
                }
                fd = msg.conn->socket_fd();
                if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
                    flush_batch(current_fd, batch);
                    current_fd = fd;
                }
                batch.append(msg.data);
            }

            flush_batch(current_fd, batch);
            current_fd = -1;
        } else {
            std::this_thread::yield();
        }
    }

    flush_batch(current_fd, batch);
}

}
