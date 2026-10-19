#include "relaymq/broker.hpp"

namespace relaymq {

void Broker::add_connection(std::shared_ptr<Connection> conn) {
    if (!conn) {
        return;
    }
    std::lock_guard<std::mutex> lock(broker_mutex_);
    connections_[conn->id()] = conn;
}

void Broker::remove_connection(const std::string& client_id) {
    std::lock_guard<std::mutex> lock(broker_mutex_);
    registry_.remove_all_for_client(client_id);
    connections_.erase(client_id);
}

void Broker::handle_command(std::shared_ptr<Connection> conn, const Command& cmd) {
    if (!conn) {
        return;
    }

    std::vector<std::pair<std::shared_ptr<Connection>, std::string>> pending_sends;
    std::unique_lock<std::mutex> lock(broker_mutex_);

    switch (cmd.type) {
        case CommandType::PUB: {
            std::vector<Subscription> matches = registry_.match(cmd.subject);
            for (size_t i = 0; i < matches.size(); i++) {
                const Subscription& sub = matches[i];
                auto it = connections_.find(sub.client_id);
                if (it != connections_.end() && it->second->is_open()) {
                    std::string msg = ProtocolParser::format_msg(cmd.subject, sub.sid, cmd.payload, cmd.reply_to);
                    pending_sends.push_back({it->second, std::move(msg)});
                }
            }
            break;
        }
        case CommandType::SUB: {
            registry_.add_subscription(cmd.subject, conn->id(), cmd.sid);
            pending_sends.push_back({conn, ProtocolParser::format_ok()});
            break;
        }
        case CommandType::UNSUB: {
            registry_.remove_subscription(conn->id(), cmd.sid);
            pending_sends.push_back({conn, ProtocolParser::format_ok()});
            break;
        }
        case CommandType::PING: {
            pending_sends.push_back({conn, ProtocolParser::format_pong()});
            break;
        }
        case CommandType::PONG:
        case CommandType::OK:
        case CommandType::ERR:
        case CommandType::UNKNOWN:
        default:
            break;
    }

    lock.unlock();

    for (size_t i = 0; i < pending_sends.size(); i++) {
        pending_sends[i].first->send_data(pending_sends[i].second);
    }
}

size_t Broker::total_connections() {
    std::lock_guard<std::mutex> lock(broker_mutex_);
    return connections_.size();
}

size_t Broker::total_subscriptions() {
    std::lock_guard<std::mutex> lock(broker_mutex_);
    return registry_.total_subscriptions();
}

void Broker::close_all_connections() {
    std::vector<std::shared_ptr<Connection>> conns;
    std::unique_lock<std::mutex> lock(broker_mutex_);
    for (auto it = connections_.begin(); it != connections_.end(); it++) {
        conns.push_back(it->second);
    }
    connections_.clear();
    lock.unlock();

    for (size_t i = 0; i < conns.size(); i++) {
        conns[i]->close_connection();
    }
}

}
