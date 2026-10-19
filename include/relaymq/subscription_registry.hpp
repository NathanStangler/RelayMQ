#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <unordered_map>

namespace relaymq {

struct Subscription {
    std::string client_id;
    std::string sid;
    std::string subject;

    bool operator==(const Subscription& other) const {
        return client_id == other.client_id && sid == other.sid;
    }
};

class SubscriptionRegistry {
public:
    SubscriptionRegistry();
    ~SubscriptionRegistry();

    void add_subscription(const std::string& subject, const std::string& client_id, const std::string& sid);
    bool remove_subscription(const std::string& client_id, const std::string& sid);
    void remove_all_for_client(const std::string& client_id);
    std::vector<Subscription> match(std::string_view publish_subject) const;
    size_t total_subscriptions() const;

    static std::vector<std::string_view> tokenize(std::string_view subject);
    static bool is_valid_subject(std::string_view subject);

private:
    struct Node {
        std::unordered_map<std::string, std::unique_ptr<Node>> literal_children;
        std::unique_ptr<Node> single_wildcard_child;
        std::vector<Subscription> multi_wildcard_subscribers;
        std::vector<Subscription> exact_subscribers;
    };

    std::unique_ptr<Node> root_;
    size_t count_ = 0;

    void match_recursive(const Node* current, const std::vector<std::string_view>& tokens, size_t token_index, std::vector<Subscription>& results) const;
    bool remove_recursive(Node* current, const std::string& client_id, const std::string& sid);
    void remove_client_recursive(Node* current, const std::string& client_id);
};

}
