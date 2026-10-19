#pragma once

#include "relaymq/connection.hpp"
#include "relaymq/subscription_registry.hpp"
#include <unordered_map>
#include <memory>
#include <mutex>
#include <string>

namespace relaymq {

class Broker {
public:
    Broker() = default;
    ~Broker() = default;

    void add_connection(std::shared_ptr<Connection> conn);
    void remove_connection(const std::string& client_id);
    void handle_command(std::shared_ptr<Connection> conn, const Command& cmd);
    size_t total_connections();
    size_t total_subscriptions();
    void close_all_connections();

private:
    SubscriptionRegistry registry_;
    std::unordered_map<std::string, std::shared_ptr<Connection>> connections_;
    std::mutex broker_mutex_;
};

}
