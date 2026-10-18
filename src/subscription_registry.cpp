#include "relaymq/subscription_registry.hpp"
#include <algorithm>
#include <sstream>

namespace relaymq {

SubscriptionRegistry::SubscriptionRegistry() : root_(std::make_unique<Node>()), count_(0) {}

SubscriptionRegistry::~SubscriptionRegistry() = default;

std::vector<std::string> SubscriptionRegistry::tokenize(const std::string& subject) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream stream(subject);

    while (std::getline(stream, token, '.')) {
        tokens.push_back(token);
    }
    return tokens;
}

bool SubscriptionRegistry::is_valid_subject(const std::string& subject) {
    if (subject.empty() || subject.front() == '.' || subject.back() == '.') {
        return false;
    }

    if (subject.find("..") != std::string::npos) {
        return false;
    }

    std::vector<std::string> tokens = tokenize(subject);
    if (tokens.empty()) {
        return false;
    }

    for (size_t i = 0; i < tokens.size(); i++) {
        const std::string& t = tokens[i];
        if (t.empty()) {
            return false;
        }

        if (t.find('>') != std::string::npos) {
            if (t != ">" || i != tokens.size() - 1) {
                return false;
            }
        }

        if (t.find('*') != std::string::npos && t != "*") {
            return false;
        }
    }

    return true;
}

void SubscriptionRegistry::add_subscription(const std::string& subject, const std::string& client_id, const std::string& sid) {
    if (!is_valid_subject(subject)) {
        return;
    }

    std::vector<std::string> tokens = tokenize(subject);
    Node* current = root_.get();

    for (size_t i = 0; i < tokens.size(); i++) {
        const std::string& token = tokens[i];

        if (token == ">") {
            Subscription sub{client_id, sid, subject};
            current->multi_wildcard_subscribers.push_back(sub);
            count_++;
            return;
        }

        if (token == "*") {
            if (!current->single_wildcard_child) {
                current->single_wildcard_child = std::make_unique<Node>();
            }
            current = current->single_wildcard_child.get();
        } else {
            auto it = current->literal_children.find(token);
            if (it == current->literal_children.end()) {
                current->literal_children[token] = std::make_unique<Node>();
                current = current->literal_children[token].get();
            } else {
                current = it->second.get();
            }
        }
    }

    Subscription sub{client_id, sid, subject};
    current->exact_subscribers.push_back(sub);
    count_++;
}

bool SubscriptionRegistry::remove_subscription(const std::string& client_id, const std::string& sid) {
    return remove_recursive(root_.get(), client_id, sid);
}

bool SubscriptionRegistry::remove_recursive(Node* current, const std::string& client_id, const std::string& sid) {
    if (!current) {
        return false;
    }

    for (auto it = current->exact_subscribers.begin(); it != current->exact_subscribers.end(); it++) {
        if (it->client_id == client_id && it->sid == sid) {
            current->exact_subscribers.erase(it);
            count_--;
            return true;
        }
    }

    for (auto it = current->multi_wildcard_subscribers.begin(); it != current->multi_wildcard_subscribers.end(); it++) {
        if (it->client_id == client_id && it->sid == sid) {
            current->multi_wildcard_subscribers.erase(it);
            count_--;
            return true;
        }
    }

    if (remove_recursive(current->single_wildcard_child.get(), client_id, sid)) {
        return true;
    }

    for (auto& pair : current->literal_children) {
        if (remove_recursive(pair.second.get(), client_id, sid)) {
            return true;
        }
    }

    return false;
}

void SubscriptionRegistry::remove_all_for_client(const std::string& client_id) {
    remove_client_recursive(root_.get(), client_id);
}

void SubscriptionRegistry::remove_client_recursive(Node* current, const std::string& client_id) {
    if (!current) {
        return;
    }

    auto exact_it = current->exact_subscribers.begin();
    while (exact_it != current->exact_subscribers.end()) {
        if (exact_it->client_id == client_id) {
            exact_it = current->exact_subscribers.erase(exact_it);
            count_--;
        } else {
            exact_it++;
        }
    }

    auto tail_it = current->multi_wildcard_subscribers.begin();
    while (tail_it != current->multi_wildcard_subscribers.end()) {
        if (tail_it->client_id == client_id) {
            tail_it = current->multi_wildcard_subscribers.erase(tail_it);
            count_--;
        } else {
            tail_it++;
        }
    }

    remove_client_recursive(current->single_wildcard_child.get(), client_id);

    for (auto& pair : current->literal_children) {
        remove_client_recursive(pair.second.get(), client_id);
    }
}

std::vector<Subscription> SubscriptionRegistry::match(const std::string& publish_subject) const {
    std::vector<Subscription> results;
    if (publish_subject.empty()) {
        return results;
    }

    const std::vector<std::string> tokens = tokenize(publish_subject);
    match_recursive(root_.get(), tokens, 0, results);
    return results;
}

void SubscriptionRegistry::match_recursive(const Node* current, const std::vector<std::string>& tokens, size_t token_index, std::vector<Subscription>& results) const {
    if (!current) {
        return;
    }

    if (token_index < tokens.size()) {
        for (const Subscription& sub : current->multi_wildcard_subscribers) {
            results.push_back(sub);
        }
    }

    if (token_index == tokens.size()) {
        for (const Subscription& sub : current->exact_subscribers) {
            results.push_back(sub);
        }
        return;
    }

    const std::string& token = tokens[token_index];

    auto it = current->literal_children.find(token);
    if (it != current->literal_children.end()) {
        match_recursive(it->second.get(), tokens, token_index + 1, results);
    }

    if (current->single_wildcard_child) {
        match_recursive(current->single_wildcard_child.get(), tokens, token_index + 1, results);
    }
}

size_t SubscriptionRegistry::total_subscriptions() const {
    return count_;
}

}
