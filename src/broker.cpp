#include "relaymq/broker.hpp"

namespace relaymq {

void Broker::set_outbound_handler(OutboundHandler handler) {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    outbound_handler_ = std::move(handler);
    lock.unlock();
}

void Broker::set_cluster_broadcast_handler(ClusterBroadcastHandler handler) {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    cluster_broadcast_handler_ = std::move(handler);
    lock.unlock();
}

void Broker::set_route_msg_handler(RouteMsgHandler handler) {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    route_msg_handler_ = std::move(handler);
    lock.unlock();
}

void Broker::set_auth_token(const std::string& token) {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    auth_token_ = token;
    lock.unlock();
}

const std::string& Broker::auth_token() const {
    return auth_token_;
}

void Broker::add_token_rule(const TokenRule& rule) {
    if (rule.token.empty()) {
        return;
    }
    std::shared_ptr<SubjectAcl> acl = std::make_shared<SubjectAcl>(false);
    for (size_t i = 0; i < rule.allow_publish.size(); i++) {
        acl->allow_publish(rule.allow_publish[i]);
    }
    for (size_t i = 0; i < rule.allow_subscribe.size(); i++) {
        acl->allow_subscribe(rule.allow_subscribe[i]);
    }

    std::unique_lock<std::mutex> lock(broker_mutex_);
    token_acls_[rule.token] = std::move(acl);
    lock.unlock();
}

void Broker::clear_token_rules() {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    token_acls_.clear();
    lock.unlock();
}

bool Broker::is_auth_required() {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    bool required = !auth_token_.empty() || !token_acls_.empty();
    lock.unlock();
    return required;
}

void Broker::add_connection(std::shared_ptr<Connection> conn) {
    if (!conn) {
        return;
    }
    std::unique_lock<std::mutex> lock(broker_mutex_);
    connections_[conn->id()] = conn;
    lock.unlock();
}

void Broker::remove_connection(const std::string& client_id) {
    std::vector<std::string> subjects_to_unsub;
    std::unique_lock<std::mutex> lock(broker_mutex_);
    std::vector<std::string> removed = registry_.remove_all_for_client(client_id);
    for (size_t i = 0; i < removed.size(); i++) {
        const std::string& subject = removed[i];
        auto it = local_subject_counts_.find(subject);
        if (it != local_subject_counts_.end()) {
            if (it->second > 0) {
                it->second--;
            }
            if (it->second == 0) {
                local_subject_counts_.erase(it);
                subjects_to_unsub.push_back(subject);
            }
        }
    }
    connections_.erase(client_id);
    lock.unlock();

    if (cluster_broadcast_handler_) {
        for (size_t i = 0; i < subjects_to_unsub.size(); i++) {
            cluster_broadcast_handler_(ProtocolParser::format_runsub(subjects_to_unsub[i]));
        }
    }
}

void Broker::deliver_local(const std::string& subject, const std::string& payload, const std::string& reply_to) {
    if (!outbound_handler_) {
        return;
    }

    std::vector<std::pair<std::shared_ptr<Connection>, std::string>> pending_msgs;
    std::unique_lock<std::mutex> lock(broker_mutex_);
    std::vector<Subscription> matches = registry_.match(subject);
    for (size_t i = 0; i < matches.size(); i++) {
        const Subscription& sub = matches[i];
        auto it = connections_.find(sub.client_id);
        if (it != connections_.end() && it->second->is_open()) {
            std::string msg = ProtocolParser::format_msg(subject, sub.sid, payload, reply_to);
            pending_msgs.emplace_back(it->second, std::move(msg));
        }
    }
    lock.unlock();

    for (size_t i = 0; i < pending_msgs.size(); i++) {
        outbound_handler_(pending_msgs[i].first, std::move(pending_msgs[i].second));
    }
}

void Broker::handle_command(std::shared_ptr<Connection> conn, const Command& cmd) {
    if (!conn || !outbound_handler_) {
        return;
    }

    bool auth_required = false;
    std::string expected_token;
    std::shared_ptr<SubjectAcl> matched_acl;
    bool token_valid = false;

    std::unique_lock<std::mutex> auth_lock(broker_mutex_);
    auth_required = !auth_token_.empty() || !token_acls_.empty();
    expected_token = auth_token_;

    if (cmd.type == CommandType::AUTH) {
        if (!expected_token.empty() && cmd.auth_token == expected_token) {
            token_valid = true;
            matched_acl = std::make_shared<SubjectAcl>(true);
        } else {
            auto it = token_acls_.find(cmd.auth_token);
            if (it != token_acls_.end()) {
                token_valid = true;
                matched_acl = it->second;
            }
        }
    }
    auth_lock.unlock();

    if (auth_required && !conn->is_authenticated()) {
        if (cmd.type == CommandType::AUTH) {
            if (token_valid) {
                conn->set_authenticated(true);
                if (matched_acl) {
                    conn->set_acl(matched_acl);
                }
                outbound_handler_(conn, ProtocolParser::format_ok());
            } else {
                outbound_handler_(conn, ProtocolParser::format_err("Invalid Credentials"));
                conn->close_connection();
            }
        } else {
            outbound_handler_(conn, ProtocolParser::format_err("Authorization Violation"));
            conn->close_connection();
        }
        return;
    }

    if (cmd.type == CommandType::AUTH) {
        if (token_valid && matched_acl) {
            conn->set_acl(matched_acl);
        }
        outbound_handler_(conn, ProtocolParser::format_ok());
        return;
    }

    switch (cmd.type) {
        case CommandType::PUB: {
            if (!conn->acl().can_publish(cmd.subject)) {
                outbound_handler_(conn, ProtocolParser::format_err("Permission Violation: Publish to " + cmd.subject + " denied"));
                break;
            }

            deliver_local(cmd.subject, cmd.payload, cmd.reply_to);

            RouteMsgHandler route_handler;
            std::unique_lock<std::mutex> lock(broker_mutex_);
            route_handler = route_msg_handler_;
            lock.unlock();

            if (route_handler) {
                route_handler(cmd.subject, cmd.payload, cmd.reply_to);
            }
            break;
        }
        case CommandType::SUB: {
            if (!conn->acl().can_subscribe(cmd.subject)) {
                outbound_handler_(conn, ProtocolParser::format_err("Permission Violation: Subscribe to " + cmd.subject + " denied"));
                break;
            }

            bool should_broadcast = false;
            std::unique_lock<std::mutex> lock(broker_mutex_);
            if (SubscriptionRegistry::is_valid_subject(cmd.subject)) {
                registry_.add_subscription(cmd.subject, conn->id(), cmd.sid);
                local_subject_counts_[cmd.subject]++;
                size_t count = local_subject_counts_[cmd.subject];
                if (count == 1) {
                    should_broadcast = true;
                }
            }
            lock.unlock();

            if (should_broadcast && cluster_broadcast_handler_) {
                cluster_broadcast_handler_(ProtocolParser::format_rsub(cmd.subject));
            }
            outbound_handler_(conn, ProtocolParser::format_ok());
            break;
        }
        case CommandType::UNSUB: {
            bool should_broadcast = false;
            std::string unsub_subject;
            std::unique_lock<std::mutex> lock(broker_mutex_);
            if (registry_.remove_subscription(conn->id(), cmd.sid, &unsub_subject)) {
                auto it = local_subject_counts_.find(unsub_subject);
                if (it != local_subject_counts_.end()) {
                    if (it->second > 0) {
                        it->second--;
                    }
                    if (it->second == 0) {
                        local_subject_counts_.erase(it);
                        should_broadcast = true;
                    }
                }
            }
            lock.unlock();

            if (should_broadcast && cluster_broadcast_handler_) {
                cluster_broadcast_handler_(ProtocolParser::format_runsub(unsub_subject));
            }
            outbound_handler_(conn, ProtocolParser::format_ok());
            break;
        }
        case CommandType::PING: {
            outbound_handler_(conn, ProtocolParser::format_pong());
            break;
        }
        case CommandType::PONG:
        case CommandType::OK:
        case CommandType::ERR:
        case CommandType::UNKNOWN:
        default:
            break;
    }
}

size_t Broker::total_connections() {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    size_t count = connections_.size();
    lock.unlock();
    return count;
}

size_t Broker::total_subscriptions() {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    size_t count = registry_.total_subscriptions();
    lock.unlock();
    return count;
}

std::vector<std::string> Broker::active_subjects() {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    std::vector<std::string> subjects;
    subjects.reserve(local_subject_counts_.size());
    for (auto it = local_subject_counts_.begin(); it != local_subject_counts_.end(); it++) {
        subjects.push_back(it->first);
    }
    lock.unlock();
    return subjects;
}

size_t Broker::local_subject_count(const std::string& subject) {
    std::unique_lock<std::mutex> lock(broker_mutex_);
    auto it = local_subject_counts_.find(subject);
    if (it != local_subject_counts_.end()) {
        size_t count = it->second;
        lock.unlock();
        return count;
    }
    lock.unlock();
    return 0;
}

void Broker::close_all_connections() {
    std::vector<std::shared_ptr<Connection>> conns;
    std::unique_lock<std::mutex> lock(broker_mutex_);
    for (auto it = connections_.begin(); it != connections_.end(); it++) {
        conns.push_back(it->second);
    }
    connections_.clear();
    local_subject_counts_.clear();
    registry_.clear();
    lock.unlock();

    for (size_t i = 0; i < conns.size(); i++) {
        conns[i]->close_connection();
    }
}

}
