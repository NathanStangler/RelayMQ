#include "relaymq/cluster.hpp"

namespace relaymq {

Cluster::~Cluster() {
    close_all();
}

void Cluster::init(const std::string& cluster_name, const std::string& node_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    cluster_name_ = cluster_name;
    node_id_ = node_id;
    lock.unlock();
}

const std::string& Cluster::cluster_name() const {
    return cluster_name_;
}

const std::string& Cluster::node_id() const {
    return node_id_;
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
            peer->set_node_id(cmd.node_id);
            peer->set_handshake_complete(true);

            std::unique_lock<std::mutex> lock(mutex_);
            node_peers_[cmd.node_id] = peer;
            lock.unlock();
            break;
        }
        case CommandType::RPING: {
            peer->send(ProtocolParser::format_rpong());
            break;
        }
        case CommandType::RPONG: {
            break;
        }
        case CommandType::RSUB:
        case CommandType::RUNSUB:
        case CommandType::RMSG:
            break;
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

void Cluster::close_all() {
    std::vector<std::shared_ptr<Peer>> peers_to_close;
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto it = peers_.begin(); it != peers_.end(); it++) {
        peers_to_close.push_back(it->second);
    }
    peers_.clear();
    node_peers_.clear();
    lock.unlock();

    for (size_t i = 0; i < peers_to_close.size(); i++) {
        peers_to_close[i]->close_connection();
    }
}

}
