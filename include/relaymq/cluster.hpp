#pragma once

#include "relaymq/peer.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace relaymq {

class Cluster {
public:
    Cluster() = default;
    ~Cluster();

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    void init(const std::string& cluster_name, const std::string& node_id);
    const std::string& cluster_name() const;
    const std::string& node_id() const;
    std::shared_ptr<Peer> add_peer(int socket_fd);
    std::shared_ptr<Peer> find_peer(int socket_fd);
    std::shared_ptr<Peer> find_peer(const std::string& node_id);
    std::shared_ptr<Peer> remove_peer(int socket_fd);
    void handle_command(std::shared_ptr<Peer> peer, const Command& cmd);
    size_t connected_peers();
    std::vector<std::string> peer_ids();
    bool send_to_peer(const std::string& node_id, std::string_view data);
    void broadcast(std::string_view data);
    void close_all();

private:
    std::string cluster_name_;
    std::string node_id_;
    std::mutex mutex_;
    std::unordered_map<int, std::shared_ptr<Peer>> peers_;
    std::unordered_map<std::string, std::shared_ptr<Peer>> node_peers_;
};

}
