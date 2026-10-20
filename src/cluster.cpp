#include "relaymq/cluster.hpp"

namespace relaymq {

Cluster::~Cluster() {
    close_all();
}

void Cluster::init(const std::string& cluster_name, const std::string& node_id, const std::string& cluster_token) {
    std::unique_lock<std::mutex> lock(mutex_);
    cluster_name_ = cluster_name;
    node_id_ = node_id;
    cluster_token_ = cluster_token;
    lock.unlock();
}

const std::string& Cluster::cluster_name() const {
    return cluster_name_;
}

const std::string& Cluster::node_id() const {
    return node_id_;
}

const std::string& Cluster::cluster_token() const {
    return cluster_token_;
}

std::shared_ptr<Peer> Cluster::add_peer(int socket_fd) {
    std::shared_ptr<Peer> peer = std::make_shared<Peer>(socket_fd, "", cluster_name_);
    std::unique_lock<std::mutex> lock(mutex_);
    peers_[socket_fd] = peer;
    lock.unlock();
    return peer;
}

std::shared_ptr<Peer> Cluster::find_peer(int socket_fd) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = peers_.find(socket_fd);
    if (it != peers_.end()) {
        std::shared_ptr<Peer> peer = it->second;
        lock.unlock();
        return peer;
    }
    lock.unlock();
    return nullptr;
}

std::shared_ptr<Peer> Cluster::find_peer(const std::string& node_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = node_peers_.find(node_id);
    if (it != node_peers_.end()) {
        std::shared_ptr<Peer> peer = it->second;
        lock.unlock();
        return peer;
    }
    lock.unlock();
    return nullptr;
}

void Cluster::set_peer_connect_handler(PeerConnectHandler handler) {
    std::unique_lock<std::mutex> lock(mutex_);
    peer_connect_handler_ = std::move(handler);
    lock.unlock();
}

void Cluster::set_inbound_route_msg_handler(InboundRouteMsgHandler handler) {
    std::unique_lock<std::mutex> lock(mutex_);
    inbound_route_msg_handler_ = std::move(handler);
    lock.unlock();
}

std::shared_ptr<Peer> Cluster::remove_peer(int socket_fd) {
    std::shared_ptr<Peer> peer;
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = peers_.find(socket_fd);
    if (it != peers_.end()) {
        peer = it->second;
        peers_.erase(it);

        std::string remote_id = peer->node_id();
        if (!remote_id.empty()) {
            node_peers_.erase(remote_id);
            peer_subscriptions_.erase(remote_id);
            remote_registry_.remove_all_for_client(remote_id);
        }
    }
    lock.unlock();

    if (peer) {
        peer->close_connection();
    }
    return peer;
}

void Cluster::handle_command(std::shared_ptr<Peer> peer, const Command& cmd) {
    if (!peer) {
        return;
    }

    switch (cmd.type) {
        case CommandType::ROUTE_CONNECT: {
            if (cmd.cluster_name != cluster_name_) {
                remove_peer(peer->socket_fd());
                return;
            }
            if (!cluster_token_.empty() && cmd.auth_token != cluster_token_) {
                remove_peer(peer->socket_fd());
                return;
            }
            peer->set_node_id(cmd.node_id);
            peer->set_handshake_complete(true);

            PeerConnectHandler connect_handler;
            std::unique_lock<std::mutex> lock(mutex_);
            node_peers_[cmd.node_id] = peer;
            connect_handler = peer_connect_handler_;
            lock.unlock();

            if (connect_handler) {
                connect_handler(peer);
            }
            break;
        }
        case CommandType::RPING: {
            peer->send(ProtocolParser::format_rpong());
            break;
        }
        case CommandType::RPONG: {
            peer->record_activity();
            break;
        }
        case CommandType::RSUB: {
            add_remote_subscription(peer->node_id(), cmd.subject);
            break;
        }
        case CommandType::RUNSUB: {
            remove_remote_subscription(peer->node_id(), cmd.subject);
            break;
        }
        case CommandType::RMSG: {
            InboundRouteMsgHandler inbound_handler;
            std::unique_lock<std::mutex> lock(mutex_);
            inbound_handler = inbound_route_msg_handler_;
            lock.unlock();

            if (inbound_handler) {
                inbound_handler(cmd.subject, cmd.payload, cmd.reply_to);
            }
            break;
        }
        default:
            break;
    }
}

size_t Cluster::connected_peers() {
    std::unique_lock<std::mutex> lock(mutex_);
    size_t count = 0;
    for (auto it = peers_.begin(); it != peers_.end(); it++) {
        if (it->second->is_connected() && it->second->is_handshake_complete()) {
            count++;
        }
    }
    lock.unlock();
    return count;
}

std::vector<std::string> Cluster::peer_ids() {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::string> ids;
    ids.reserve(node_peers_.size());
    for (auto it = node_peers_.begin(); it != node_peers_.end(); it++) {
        ids.push_back(it->first);
    }
    lock.unlock();
    return ids;
}

std::vector<std::shared_ptr<Peer>> Cluster::all_peers() {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<Peer>> list;
    list.reserve(peers_.size());
    for (auto it = peers_.begin(); it != peers_.end(); it++) {
        list.push_back(it->second);
    }
    lock.unlock();
    return list;
}

bool Cluster::send_to_peer(const std::string& node_id, std::string_view data) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = node_peers_.find(node_id);
    if (it != node_peers_.end()) {
        std::shared_ptr<Peer> peer = it->second;
        lock.unlock();
        return peer->send(data);
    }
    lock.unlock();
    return false;
}

void Cluster::broadcast(std::string_view data) {
    std::vector<std::shared_ptr<Peer>> active_peers;
    std::unique_lock<std::mutex> lock(mutex_);
    active_peers.reserve(peers_.size());
    for (auto it = peers_.begin(); it != peers_.end(); it++) {
        if (it->second->is_connected() && it->second->is_handshake_complete()) {
            active_peers.push_back(it->second);
        }
    }
    lock.unlock();

    for (size_t i = 0; i < active_peers.size(); i++) {
        active_peers[i]->send(data);
    }
}

void Cluster::route_message(const std::string& subject, const std::string& payload, const std::string& reply_to) {
    std::vector<std::string> target_peers = match_peers(subject);
    if (target_peers.empty()) {
        return;
    }
    std::string rmsg = ProtocolParser::format_rmsg(subject, payload, reply_to);
    for (size_t i = 0; i < target_peers.size(); i++) {
        send_to_peer(target_peers[i], rmsg);
    }
}

void Cluster::add_remote_subscription(const std::string& node_id, const std::string& subject) {
    if (node_id.empty() || !SubscriptionRegistry::is_valid_subject(subject)) {
        return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if (peer_subscriptions_[node_id].insert(subject).second) {
        remote_registry_.add_subscription(subject, node_id, subject);
    }
    lock.unlock();
}

bool Cluster::remove_remote_subscription(const std::string& node_id, const std::string& subject) {
    if (node_id.empty()) {
        return false;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = peer_subscriptions_.find(node_id);
    if (it != peer_subscriptions_.end() && it->second.erase(subject) > 0) {
        if (it->second.empty()) {
            peer_subscriptions_.erase(it);
        }
        remote_registry_.remove_subscription(node_id, subject);
        lock.unlock();
        return true;
    }
    lock.unlock();
    return false;
}

void Cluster::remove_all_remote_subscriptions(const std::string& node_id) {
    if (node_id.empty()) {
        return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    peer_subscriptions_.erase(node_id);
    remote_registry_.remove_all_for_client(node_id);
    lock.unlock();
}

std::vector<std::string> Cluster::match_peers(std::string_view subject) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<Subscription> matches = remote_registry_.match(subject);
    lock.unlock();

    std::vector<std::string> unique_peers;
    unique_peers.reserve(matches.size());
    for (size_t i = 0; i < matches.size(); i++) {
        const std::string& peer_id = matches[i].client_id;
        if (std::find(unique_peers.begin(), unique_peers.end(), peer_id) == unique_peers.end()) {
            unique_peers.push_back(peer_id);
        }
    }
    return unique_peers;
}

size_t Cluster::total_remote_subscriptions() {
    std::unique_lock<std::mutex> lock(mutex_);
    size_t count = remote_registry_.total_subscriptions();
    lock.unlock();
    return count;
}

std::vector<std::string> Cluster::peer_subscriptions(const std::string& node_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::string> subscriptions;
    auto it = peer_subscriptions_.find(node_id);
    if (it != peer_subscriptions_.end()) {
        subscriptions.reserve(it->second.size());
        for (auto sit = it->second.begin(); sit != it->second.end(); sit++) {
            subscriptions.push_back(*sit);
        }
    }
    lock.unlock();
    return subscriptions;
}

void Cluster::close_all() {
    std::vector<std::shared_ptr<Peer>> peers_to_close;
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto it = peers_.begin(); it != peers_.end(); it++) {
        peers_to_close.push_back(it->second);
    }
    peers_.clear();
    node_peers_.clear();
    peer_subscriptions_.clear();
    remote_registry_.clear();
    lock.unlock();

    for (size_t i = 0; i < peers_to_close.size(); i++) {
        peers_to_close[i]->close_connection();
    }
}

}
