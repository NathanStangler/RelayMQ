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

void Broker::handle_command(std::shared_ptr<Connection> conn, const Command& cmd) {
    if (!conn || !outbound_handler_) {
        return;
    }

    switch (cmd.type) {
        case CommandType::PUB: {
            std::vector<std::pair<std::shared_ptr<Connection>, std::string>> pending_msgs;
            std::unique_lock<std::mutex> lock(broker_mutex_);
            std::vector<Subscription> matches = registry_.match(cmd.subject);
            for (size_t i = 0; i < matches.size(); i++) {
                const Subscription& sub = matches[i];
                auto it = connections_.find(sub.client_id);
                if (it != connections_.end() && it->second->is_open()) {
                    std::string msg = ProtocolParser::format_msg(cmd.subject, sub.sid, cmd.payload, cmd.reply_to);
                    pending_msgs.emplace_back(it->second, std::move(msg));
                }
            }
            lock.unlock();

            for (size_t i = 0; i < pending_msgs.size(); i++) {
                outbound_handler_(pending_msgs[i].first, std::move(pending_msgs[i].second));
            }
            break;
        }
        case CommandType::SUB: {
            bool should_broadcast = false;
            std::unique_lock<std::mutex> lock(broker_mutex_);
            if (SubscriptionRegistry::is_valid_subject(cmd.subject)) {
                registry_.add_subscription(cmd.subject, conn->id(), cmd.sid);
                size_t count = ++local_subject_counts_[cmd.subject];
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
