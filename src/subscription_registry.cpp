#include "relaymq/subscription_registry.hpp"
#include <algorithm>

namespace relaymq {

SubscriptionRegistry::SubscriptionRegistry() : root_(std::make_unique<Node>()), count_(0) {}

SubscriptionRegistry::~SubscriptionRegistry() = default;

void SubscriptionRegistry::clear() {
    root_ = std::make_unique<Node>();
    count_ = 0;
}

std::vector<std::string_view> SubscriptionRegistry::tokenize(std::string_view subject) {
    std::vector<std::string_view> tokens;
    if (subject.empty()) {
        return tokens;
    }

    size_t start = 0;
    size_t end = subject.find('.');

    while (end != std::string_view::npos) {
        tokens.push_back(subject.substr(start, end - start));
        start = end + 1;
        end = subject.find('.', start);
    }
    tokens.push_back(subject.substr(start));
    return tokens;
}

bool SubscriptionRegistry::is_valid_subject(std::string_view subject) {
    if (subject.empty() || subject.front() == '.' || subject.back() == '.') {
        return false;
    }

    if (subject.find("..") != std::string_view::npos) {
        return false;
    }

    for (char c : subject) {
        if (static_cast<unsigned char>(c) <= 32 || static_cast<unsigned char>(c) == 127) {
            return false;
        }
    }

    std::vector<std::string_view> tokens = tokenize(subject);
    if (tokens.empty()) {
        return false;
    }

    for (size_t i = 0; i < tokens.size(); i++) {
        std::string_view t = tokens[i];
        if (t.empty()) {
            return false;
        }

        if (t.find('>') != std::string_view::npos) {
            if (t != ">" || i != tokens.size() - 1) {
                return false;
            }
        }

        if (t.find('*') != std::string_view::npos && t != "*") {
            return false;
        }
    }

    return true;
}

bool SubscriptionRegistry::is_valid_publish_subject(std::string_view subject) {
    if (!is_valid_subject(subject)) {
        return false;
    }
    if (subject.find('*') != std::string_view::npos || subject.find('>') != std::string_view::npos) {
        return false;
    }
    return true;
}

void SubscriptionRegistry::add_subscription(const std::string& subject, const std::string& client_id, const std::string& sid) {
    if (!is_valid_subject(subject)) {
        return;
    }

    std::vector<std::string_view> tokens = tokenize(subject);
    Node* current = root_.get();

    for (size_t i = 0; i < tokens.size(); i++) {
        std::string_view token = tokens[i];

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
            std::string key(token);
            auto it = current->literal_children.find(key);
            if (it == current->literal_children.end()) {
                current->literal_children[key] = std::make_unique<Node>();
                current = current->literal_children[key].get();
            } else {
                current = it->second.get();
            }
        }
    }

    Subscription sub{client_id, sid, subject};
    current->exact_subscribers.push_back(sub);
    count_++;
}

bool SubscriptionRegistry::remove_subscription(const std::string& client_id, const std::string& sid, std::string* subject) {
    return remove_recursive(root_.get(), client_id, sid, subject);
}

bool SubscriptionRegistry::remove_recursive(Node* current, const std::string& client_id, const std::string& sid, std::string* subject) {
    if (!current) {
        return false;
    }

    for (auto it = current->exact_subscribers.begin(); it != current->exact_subscribers.end(); it++) {
        if (it->client_id == client_id && it->sid == sid) {
            if (subject) {
                *subject = it->subject;
            }
            current->exact_subscribers.erase(it);
            count_--;
            return true;
        }
    }

    for (auto it = current->multi_wildcard_subscribers.begin(); it != current->multi_wildcard_subscribers.end(); it++) {
        if (it->client_id == client_id && it->sid == sid) {
            if (subject) {
                *subject = it->subject;
            }
            current->multi_wildcard_subscribers.erase(it);
            count_--;
            return true;
        }
    }

    if (remove_recursive(current->single_wildcard_child.get(), client_id, sid, subject)) {
        return true;
    }

    for (auto& pair : current->literal_children) {
        if (remove_recursive(pair.second.get(), client_id, sid, subject)) {
            return true;
        }
    }

    return false;
}

std::vector<std::string> SubscriptionRegistry::remove_all_for_client(const std::string& client_id) {
    std::vector<std::string> removed_subjects;
    remove_client_recursive(root_.get(), client_id, removed_subjects);
    return removed_subjects;
}

void SubscriptionRegistry::remove_client_recursive(Node* current, const std::string& client_id, std::vector<std::string>& removed_subjects) {
    if (!current) {
        return;
    }

    auto exact_it = current->exact_subscribers.begin();
    while (exact_it != current->exact_subscribers.end()) {
        if (exact_it->client_id == client_id) {
            removed_subjects.push_back(exact_it->subject);
            exact_it = current->exact_subscribers.erase(exact_it);
            count_--;
        } else {
            exact_it++;
        }
    }

    auto tail_it = current->multi_wildcard_subscribers.begin();
    while (tail_it != current->multi_wildcard_subscribers.end()) {
        if (tail_it->client_id == client_id) {
            removed_subjects.push_back(tail_it->subject);
            tail_it = current->multi_wildcard_subscribers.erase(tail_it);
            count_--;
        } else {
            tail_it++;
        }
    }

    remove_client_recursive(current->single_wildcard_child.get(), client_id, removed_subjects);

    for (auto& pair : current->literal_children) {
        remove_client_recursive(pair.second.get(), client_id, removed_subjects);
    }
}

std::vector<Subscription> SubscriptionRegistry::match(std::string_view publish_subject) const {
    std::vector<Subscription> results;
    if (publish_subject.empty()) {
        return results;
    }

    const std::vector<std::string_view> tokens = tokenize(publish_subject);
    match_recursive(root_.get(), tokens, 0, results);
    return results;
}

void SubscriptionRegistry::match_recursive(const Node* current, const std::vector<std::string_view>& tokens, size_t token_index, std::vector<Subscription>& results) const {
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

    std::string_view token = tokens[token_index];

    auto it = current->literal_children.find(std::string(token));
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
