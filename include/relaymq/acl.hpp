#pragma once

#include "relaymq/subscription_registry.hpp"
#include <string>
#include <string_view>
#include <vector>

namespace relaymq {

class SubjectAcl {
public:
    SubjectAcl(bool unrestricted = true);
    ~SubjectAcl() = default;

    bool allow_publish(const std::string& pattern);
    bool allow_subscribe(const std::string& pattern);
    bool can_publish(std::string_view subject) const;
    bool can_subscribe(std::string_view subject) const;
    bool is_unrestricted() const;
    void clear();
    size_t publish_rule_count() const;
    size_t subscribe_rule_count() const;
    const std::vector<std::string>& publish_patterns() const;
    const std::vector<std::string>& subscribe_patterns() const;

private:
    bool unrestricted_;
    bool allow_all_publish_ = false;
    bool allow_all_subscribe_ = false;
    std::vector<std::string> publish_patterns_;
    std::vector<std::string> subscribe_patterns_;
    SubscriptionRegistry publish_registry_;
    SubscriptionRegistry subscribe_registry_;
};

}
