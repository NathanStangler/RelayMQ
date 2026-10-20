#pragma once

#include "relaymq/peer.hpp"
#include "relaymq/subscription_registry.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <functional>

namespace relaymq {

class Cluster {
public:
    using PeerConnectHandler = std::function<void(std::shared_ptr<Peer>)>;
    using InboundRouteMsgHandler = std::function<void(const std::string& subject, const std::string& payload, const std::string& reply_to)>;

    Cluster() = default;
    ~Cluster();

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    void init(const std::string& cluster_name, const std::string& node_id, const std::string& cluster_token = "");
    const std::string& cluster_name() const;
    const std::string& node_id() const;
    const std::string& cluster_token() const;
    void set_peer_connect_handler(PeerConnectHandler handler);
    void set_inbound_route_msg_handler(InboundRouteMsgHandler handler);
    std::shared_ptr<Peer> add_peer(int socket_fd);
    std::shared_ptr<Peer> find_peer(int socket_fd);
    std::shared_ptr<Peer> find_peer(const std::string& node_id);
    std::shared_ptr<Peer> remove_peer(int socket_fd);
    void handle_command(std::shared_ptr<Peer> peer, const Command& cmd);
    size_t connected_peers();
    std::vector<std::string> peer_ids();
    std::vector<std::shared_ptr<Peer>> all_peers();
    bool send_to_peer(const std::string& node_id, std::string_view data);
    void broadcast(std::string_view data);
    void route_message(const std::string& subject, const std::string& payload, const std::string& reply_to = "");
    void add_remote_subscription(const std::string& node_id, const std::string& subject);
    bool remove_remote_subscription(const std::string& node_id, const std::string& subject);
    void remove_all_remote_subscriptions(const std::string& node_id);
    std::vector<std::string> match_peers(std::string_view subject);
    size_t total_remote_subscriptions();
    std::vector<std::string> peer_subscriptions(const std::string& node_id);
    void close_all();

private:
    std::string cluster_name_;
    std::string node_id_;
    std::string cluster_token_;
    std::mutex mutex_;
    PeerConnectHandler peer_connect_handler_;
    InboundRouteMsgHandler inbound_route_msg_handler_;
    std::unordered_map<int, std::shared_ptr<Peer>> peers_;
    std::unordered_map<std::string, std::shared_ptr<Peer>> node_peers_;
    std::unordered_map<std::string, std::unordered_set<std::string>> peer_subscriptions_;
    SubscriptionRegistry remote_registry_;
};

}
