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
#include <chrono>

namespace relaymq {

Server::Server()
    : server_fd_(-1), cluster_server_fd_(-1), epoll_fd_(-1), stop_event_fd_(-1), port_(0), cluster_port_(0), is_running_(false), next_client_id_(1) {}

Server::~Server() {
    stop();
}

bool Server::start(const ServerConfig& config) {
    bool expected = false;
    if (!is_running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }

    std::string assigned_node_id;
    if (!config.node_id.empty()) {
        assigned_node_id = config.node_id;
    } else {
        assigned_node_id = "node_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }
    cluster_.init(config.cluster_name, assigned_node_id);

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int flags = fcntl(server_fd_, F_GETFL, 0);
    fcntl(server_fd_, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(config.port);

    if (bind(server_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    if (listen(server_fd_, 128) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    socklen_t addr_len = sizeof(server_addr);
    if (getsockname(server_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), &addr_len) == 0) {
        port_ = ntohs(server_addr.sin_port);
    } else {
        port_ = config.port;
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        close(server_fd_);
        server_fd_ = -1;
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    stop_event_fd_ = eventfd(0, EFD_NONBLOCK);
    if (stop_event_fd_ < 0) {
        close(epoll_fd_);
        close(server_fd_);
        epoll_fd_ = -1;
        server_fd_ = -1;
        is_running_.store(false, std::memory_order_release);
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

    if (config.enable_clustering || config.cluster_port > 0 || !config.peers.empty()) {
        cluster_server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (cluster_server_fd_ >= 0) {
            setsockopt(cluster_server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            int cflags = fcntl(cluster_server_fd_, F_GETFL, 0);
            fcntl(cluster_server_fd_, F_SETFL, cflags | O_NONBLOCK);

            sockaddr_in cluster_addr{};
            cluster_addr.sin_family = AF_INET;
            cluster_addr.sin_addr.s_addr = INADDR_ANY;
            cluster_addr.sin_port = htons(config.cluster_port);

            if (bind(cluster_server_fd_, reinterpret_cast<struct sockaddr*>(&cluster_addr), sizeof(cluster_addr)) == 0 &&
                listen(cluster_server_fd_, 64) == 0) {
                socklen_t clen = sizeof(cluster_addr);
                if (getsockname(cluster_server_fd_, reinterpret_cast<struct sockaddr*>(&cluster_addr), &clen) == 0) {
                    cluster_port_ = ntohs(cluster_addr.sin_port);
                } else {
                    cluster_port_ = config.cluster_port;
                }

                epoll_event cluster_ev{};
                cluster_ev.events = EPOLLIN;
                cluster_ev.data.fd = cluster_server_fd_;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cluster_server_fd_, &cluster_ev);
            } else {
                close(cluster_server_fd_);
                cluster_server_fd_ = -1;
            }
        }
    }

    broker_.set_outbound_handler([this](std::shared_ptr<Connection> conn, std::string data) {
        OutboundMessage msg{std::move(conn), std::move(data)};
        while (is_running_.load(std::memory_order_relaxed) && !outbound_queue_.push(std::move(msg))) {
            std::this_thread::yield();
        }
    });

    writer_thread_ = std::thread(&Server::writer_loop, this);
    event_thread_ = std::thread(&Server::event_loop, this);

    for (size_t i = 0; i < config.peers.size(); i++) {
        const std::string& peer_str = config.peers[i];
        size_t colon = peer_str.find(':');
        if (colon != std::string::npos) {
            std::string host = peer_str.substr(0, colon);
            uint16_t port = static_cast<uint16_t>(std::stoi(peer_str.substr(colon + 1)));
            connect_to_peer(host, port);
        }
    }

    return true;
}

void Server::stop() {
    bool expected = true;
    if (!is_running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        return;
    }

    if (stop_event_fd_ >= 0) {
        uint64_t val = 1;
        ssize_t written = write(stop_event_fd_, &val, sizeof(val));
        (void)written;
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

    if (cluster_server_fd_ >= 0) {
        shutdown(cluster_server_fd_, SHUT_RDWR);
        close(cluster_server_fd_);
        cluster_server_fd_ = -1;
    }

    cluster_.close_all();
    broker_.close_all_connections();
    clients_.clear();
}

bool Server::is_running() const {
    return is_running_.load(std::memory_order_acquire);
}

uint16_t Server::port() const {
    return port_;
}

uint16_t Server::cluster_port() const {
    return cluster_port_;
}

const std::string& Server::node_id() const {
    return cluster_.node_id();
}

const std::string& Server::cluster_name() const {
    return cluster_.cluster_name();
}

Broker& Server::broker() {
    return broker_;
}

Cluster& Server::cluster() {
    return cluster_;
}

bool Server::connect_to_peer(const std::string& host, uint16_t port) {
    if (!is_running_.load(std::memory_order_relaxed)) {
        return false;
    }

    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0) {
        return false;
    }

    int nodelay = 1;
    setsockopt(peer_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    sockaddr_in peer_addr{};
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &peer_addr.sin_addr) <= 0) {
        close(peer_fd);
        return false;
    }

    if (::connect(peer_fd, reinterpret_cast<struct sockaddr*>(&peer_addr), sizeof(peer_addr)) < 0) {
        close(peer_fd);
        return false;
    }

    int flags = fcntl(peer_fd, F_GETFL, 0);
    fcntl(peer_fd, F_SETFL, flags | O_NONBLOCK);

    std::shared_ptr<Peer> peer = cluster_.add_peer(peer_fd);

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = peer_fd;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, peer_fd, &ev);

    peer->send(ProtocolParser::format_route_connect(cluster_.cluster_name(), cluster_.node_id()));
    return true;
}

void Server::accept_cluster_connection() {
    while (is_running_.load(std::memory_order_relaxed)) {
        sockaddr_in peer_addr{};
        socklen_t peer_len = sizeof(peer_addr);

        int peer_fd = accept4(cluster_server_fd_, reinterpret_cast<struct sockaddr*>(&peer_addr), &peer_len, SOCK_NONBLOCK);
        if (peer_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        int nodelay = 1;
        setsockopt(peer_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        std::shared_ptr<Peer> peer = cluster_.add_peer(peer_fd);

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = peer_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, peer_fd, &ev);

        peer->send(ProtocolParser::format_route_connect(cluster_.cluster_name(), cluster_.node_id()));
    }
}

void Server::event_loop() {
    epoll_event events[64];

    while (is_running_.load(std::memory_order_relaxed)) {
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
            } else if (cluster_server_fd_ >= 0 && fd == cluster_server_fd_) {
                accept_cluster_connection();
            } else if (cluster_.find_peer(fd)) {
                handle_peer(fd, ev_flags);
            } else {
                handle_client(fd, ev_flags);
            }
        }
    }
}

void Server::accept_connection() {
    while (is_running_.load(std::memory_order_relaxed)) {
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

void Server::handle_peer(int peer_fd, uint32_t events) {
    std::shared_ptr<Peer> peer = cluster_.find_peer(peer_fd);
    if (!peer) {
        return;
    }

    if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        remove_peer(peer_fd);
        return;
    }

    char buffer[16384];
    ssize_t bytes_read = recv(peer_fd, buffer, sizeof(buffer), MSG_DONTWAIT);
    if (bytes_read > 0) {
        peer->parser().append(buffer, static_cast<size_t>(bytes_read));
        Command cmd;
        while (peer->parser().next_command(cmd)) {
            cluster_.handle_command(peer, cmd);
        }
    } else if (bytes_read == 0) {
        remove_peer(peer_fd);
    } else {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            remove_peer(peer_fd);
        }
    }
}

void Server::remove_peer(int peer_fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, peer_fd, nullptr);
    cluster_.remove_peer(peer_fd);
}

size_t Server::connected_peers() {
    return cluster_.connected_peers();
}

std::vector<std::string> Server::peer_ids() {
    return cluster_.peer_ids();
}

bool Server::send_to_peer(const std::string& node_id, std::string_view data) {
    return cluster_.send_to_peer(node_id, data);
}

void Server::broadcast_to_peers(std::string_view data) {
    cluster_.broadcast(data);
}

void Server::flush_batch(int fd, std::string& batch) {
    if (fd < 0 || batch.empty()) {
        return;
    }

    size_t total_sent = 0;
    int retries = 0;
    while (is_running_.load(std::memory_order_relaxed) && total_sent < batch.size()) {
        ssize_t sent = send(fd, batch.data() + total_sent, batch.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                retries++;
                if (retries > 1000) {
                    break;
                }
                std::this_thread::yield();
                continue;
            }
            break;
        }
        total_sent += static_cast<size_t>(sent);
        retries = 0;
    }
    batch.clear();
}

void Server::writer_loop() {
    std::string batch;
    batch.reserve(65536);
    int current_fd = -1;

    while (is_running_.load(std::memory_order_relaxed)) {
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

    OutboundMessage msg;
    while (outbound_queue_.pop(msg)) {
        if (!msg.conn || !msg.conn->is_open()) {
            continue;
        }
        int fd = msg.conn->socket_fd();
        if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
            flush_batch(current_fd, batch);
            current_fd = fd;
        }
        batch.append(msg.data);
    }

    flush_batch(current_fd, batch);
}

}
