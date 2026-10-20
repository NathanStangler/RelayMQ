#pragma once

#include "relaymq/connection.hpp"
#include "relaymq/subscription_registry.hpp"
#include <unordered_map>
#include <memory>
#include <mutex>
#include <string>
#include <functional>

namespace relaymq {

class Broker {
public:
    using OutboundHandler = std::function<void(std::shared_ptr<Connection>, std::string)>;
    using ClusterBroadcastHandler = std::function<void(std::string_view)>;
    using RouteMsgHandler = std::function<void(const std::string& subject, const std::string& payload, const std::string& reply_to)>;

    Broker() = default;
    ~Broker() = default;

    void set_outbound_handler(OutboundHandler handler);
    void set_cluster_broadcast_handler(ClusterBroadcastHandler handler);
    void set_route_msg_handler(RouteMsgHandler handler);
    void set_auth_token(const std::string& token);
    const std::string& auth_token() const;
    bool is_auth_required() const;
    void add_connection(std::shared_ptr<Connection> conn);
    void remove_connection(const std::string& client_id);
    void handle_command(std::shared_ptr<Connection> conn, const Command& cmd);
    void deliver_local(const std::string& subject, const std::string& payload, const std::string& reply_to = "");
    size_t total_connections();
    size_t total_subscriptions();
    std::vector<std::string> active_subjects();
    size_t local_subject_count(const std::string& subject);
    void close_all_connections();

private:
    SubscriptionRegistry registry_;
    std::unordered_map<std::string, std::shared_ptr<Connection>> connections_;
    std::unordered_map<std::string, size_t> local_subject_counts_;
    std::mutex broker_mutex_;
    OutboundHandler outbound_handler_;
    ClusterBroadcastHandler cluster_broadcast_handler_;
    RouteMsgHandler route_msg_handler_;
    std::string auth_token_;
};

}
