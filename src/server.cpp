#include "relaymq/server.hpp"
#include "relaymq/net.hpp"

#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
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

    std::string assigned_node_id = config.node_id.empty()
        ? "node_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())
        : config.node_id;
    cluster_.init(config.cluster_name, assigned_node_id, config.cluster_token);
    broker_.set_auth_token(config.auth_token);
    for (size_t i = 0; i < config.token_rules.size(); i++) {
        broker_.add_token_rule(config.token_rules[i]);
    }

    server_fd_ = net::create_tcp_listener(config.port, 128, &port_);
    if (server_fd_ < 0) {
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    bool clustering_active = config.enable_clustering || config.cluster_port > 0 || !config.peers.empty();
    if (clustering_active) {
        cluster_server_fd_ = net::create_tcp_listener(config.cluster_port, 64, &cluster_port_);
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        net::close_socket(server_fd_);
        net::close_socket(cluster_server_fd_);
        is_running_.store(false, std::memory_order_release);
        return false;
    }

    stop_event_fd_ = eventfd(0, EFD_NONBLOCK);
    if (stop_event_fd_ < 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
        net::close_socket(server_fd_);
        net::close_socket(cluster_server_fd_);
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

    if (cluster_server_fd_ >= 0) {
        epoll_event cluster_ev{};
        cluster_ev.events = EPOLLIN;
        cluster_ev.data.fd = cluster_server_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cluster_server_fd_, &cluster_ev);
    }

    writer_.start();

    broker_.set_outbound_handler([this](std::shared_ptr<Connection> conn, std::string data, bool close_after) {
        writer_.enqueue(std::move(conn), std::move(data), close_after);
    });

    broker_.set_cluster_broadcast_handler([this](std::string_view data) {
        cluster_.broadcast(data);
    });

    broker_.set_route_msg_handler([this](const std::string& subject, const std::string& payload, const std::string& reply_to) {
        cluster_.route_message(subject, payload, reply_to);
    });

    cluster_.set_peer_connect_handler([this](std::shared_ptr<Peer> peer) {
        if (!peer) {
            return;
        }
        std::vector<std::string> subjects = broker_.active_subjects();
        for (size_t i = 0; i < subjects.size(); i++) {
            peer->send(ProtocolParser::format_rsub(subjects[i]));
        }
    });

    cluster_.set_inbound_route_msg_handler([this](const std::string& subject, const std::string& payload, const std::string& reply_to) {
        broker_.deliver_local(subject, payload, reply_to);
    });

    config_ = config;
    event_thread_ = std::thread(&Server::event_loop, this);

    if (clustering_active && config_.heartbeat_interval_ms > 0) {
        heartbeat_thread_ = std::thread(&Server::heartbeat_loop, this);
    }

    for (size_t i = 0; i < config.peers.size(); i++) {
        Endpoint endpoint;
        if (Endpoint::parse(config.peers[i], endpoint)) {
            connect_to_peer(endpoint.host, endpoint.port);
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

    writer_.stop();

    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (stop_event_fd_ >= 0) {
        close(stop_event_fd_);
        stop_event_fd_ = -1;
    }

    net::close_socket(server_fd_);
    net::close_socket(cluster_server_fd_);

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

    int peer_fd = net::connect_tcp(host, port, true);
    if (peer_fd < 0) {
        return false;
    }

    std::shared_ptr<Peer> peer = cluster_.add_peer(peer_fd);
    peer->set_endpoint(host, port);

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = peer_fd;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, peer_fd, &ev);

    peer->send(ProtocolParser::format_route_connect(cluster_.cluster_name(), cluster_.node_id(), cluster_.cluster_token()));
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

        net::set_tcp_nodelay(peer_fd, true);

        std::shared_ptr<Peer> peer = cluster_.add_peer(peer_fd);
        char peer_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
        peer->set_endpoint(peer_ip, ntohs(peer_addr.sin_port));

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = peer_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, peer_fd, &ev);

        peer->send(ProtocolParser::format_route_connect(cluster_.cluster_name(), cluster_.node_id(), cluster_.cluster_token()));
    }
}

void Server::event_loop() {
    epoll_event events[64];

    while (is_running_.load(std::memory_order_relaxed)) {
        bool auth_required = broker_.is_auth_required();
        int timeout_ms = (auth_required && config_.auth_timeout_ms > 0) ? 500 : -1;
        int num_ready = epoll_wait(epoll_fd_, events, 64, timeout_ms);
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

        if (auth_required && config_.auth_timeout_ms > 0) {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            std::vector<int> expired_clients;
            for (auto it = clients_.begin(); it != clients_.end(); it++) {
                if (!it->second->is_authenticated()) {
                    int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second->connected_at()).count();
                    if (elapsed_ms > config_.auth_timeout_ms) {
                        expired_clients.push_back(it->first);
                    }
                }
            }
            for (size_t i = 0; i < expired_clients.size(); i++) {
                remove_client(expired_clients[i]);
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

        net::set_tcp_nodelay(client_fd, true);

        uint64_t cid = next_client_id_++;
        std::string client_id = "client_" + std::to_string(cid);

        bool authenticated = !broker_.is_auth_required();
        std::shared_ptr<Connection> conn = std::make_shared<Connection>(client_fd, client_id, authenticated);
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
            if (!conn->is_open()) {
                remove_client(client_fd);
                return;
            }
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
        peer->record_activity();
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

std::vector<std::string> Server::match_peers(std::string_view subject) {
    return cluster_.match_peers(subject);
}

size_t Server::total_remote_subscriptions() {
    return cluster_.total_remote_subscriptions();
}

std::vector<std::string> Server::peer_subscriptions(const std::string& node_id) {
    return cluster_.peer_subscriptions(node_id);
}

std::vector<std::string> Server::active_subjects() {
    return broker_.active_subjects();
}

size_t Server::local_subject_count(const std::string& subject) {
    return broker_.local_subject_count(subject);
}

void Server::heartbeat_loop() {
    int interval_ms = config_.heartbeat_interval_ms;
    int max_missed = config_.max_missed_heartbeats;

    while (is_running_.load(std::memory_order_relaxed)) {
        std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms);
        while (is_running_.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (!is_running_.load(std::memory_order_relaxed)) {
            break;
        }

        std::vector<int> dead_fds;
        std::vector<std::shared_ptr<Peer>> peers = cluster_.all_peers();
        for (size_t i = 0; i < peers.size(); i++) {
            std::shared_ptr<Peer>& peer = peers[i];
            if (!peer->is_connected() || !peer->is_handshake_complete()) {
                continue;
            }

            int missed = peer->increment_unanswered_pings();
            if (missed > max_missed) {
                dead_fds.push_back(peer->socket_fd());
            } else {
                peer->send(ProtocolParser::format_rping());
            }
        }

        for (size_t i = 0; i < dead_fds.size(); i++) {
            remove_peer(dead_fds[i]);
        }

        for (size_t i = 0; i < config_.peers.size(); i++) {
            Endpoint endpoint;
            if (Endpoint::parse(config_.peers[i], endpoint)) {
                bool already_connected = false;
                std::vector<std::shared_ptr<Peer>> active = cluster_.all_peers();
                for (size_t j = 0; j < active.size(); j++) {
                    if (active[j]->is_connected() && active[j]->endpoint() == endpoint.to_string()) {
                        already_connected = true;
                        break;
                    }
                }

                if (!already_connected) {
                    connect_to_peer(endpoint.host, endpoint.port);
                }
            }
        }
    }
}

}
