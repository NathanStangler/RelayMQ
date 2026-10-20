#pragma once

#include "relaymq/broker.hpp"
#include "relaymq/spsc_queue.hpp"
#include "relaymq/cluster.hpp"
#include <atomic>
#include <thread>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace relaymq {

struct OutboundMessage {
    std::shared_ptr<Connection> conn;
    std::string data;
};

struct ServerConfig {
    uint16_t port = 9000;
    bool enable_clustering = false;
    uint16_t cluster_port = 0;
    std::string cluster_name = "default";
    std::string node_id = "";
    std::vector<std::string> peers;
    int heartbeat_interval_ms = 1000;
    int max_missed_heartbeats = 3;
    std::string auth_token = "";
    std::string cluster_token = "";
    int auth_timeout_ms = 2000;
};

class Server {
public:
    Server();
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start(const ServerConfig& config = {});
    void stop();
    bool is_running() const;
    uint16_t port() const;
    uint16_t cluster_port() const;
    const std::string& node_id() const;
    const std::string& cluster_name() const;
    Broker& broker();
    Cluster& cluster();
    bool connect_to_peer(const std::string& host, uint16_t port);
    size_t connected_peers();
    std::vector<std::string> peer_ids();
    bool send_to_peer(const std::string& node_id, std::string_view data);
    void broadcast_to_peers(std::string_view data);
    std::vector<std::string> match_peers(std::string_view subject);
    size_t total_remote_subscriptions();
    std::vector<std::string> peer_subscriptions(const std::string& node_id);
    std::vector<std::string> active_subjects();
    size_t local_subject_count(const std::string& subject);

private:
    int server_fd_;
    int cluster_server_fd_;
    int epoll_fd_;
    int stop_event_fd_;
    uint16_t port_;
    uint16_t cluster_port_;
    std::atomic<bool> is_running_;
    Broker broker_;
    Cluster cluster_;
    ServerConfig config_;
    std::thread event_thread_;
    std::thread writer_thread_;
    std::thread heartbeat_thread_;
    SpscQueue<OutboundMessage, 65536> outbound_queue_;
    std::unordered_map<int, std::shared_ptr<Connection>> clients_;
    std::atomic<uint64_t> next_client_id_;

    void event_loop();
    void writer_loop();
    void heartbeat_loop();
    void flush_batch(int fd, std::string& batch);
    void accept_connection();
    void handle_client(int client_fd, uint32_t events);
    void remove_client(int client_fd);
    void accept_cluster_connection();
    void handle_peer(int peer_fd, uint32_t events);
    void remove_peer(int peer_fd);
};

}
