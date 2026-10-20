#include "relaymq/acl.hpp"

namespace relaymq {

SubjectAcl::SubjectAcl(bool unrestricted) : unrestricted_(unrestricted) {}

bool SubjectAcl::allow_publish(const std::string& pattern) {
    if (!SubscriptionRegistry::is_valid_subject(pattern)) {
        return false;
    }
    unrestricted_ = false;
    if (pattern == ">") {
        allow_all_publish_ = true;
    }
    publish_patterns_.push_back(pattern);
    publish_registry_.add_subscription(pattern, "acl", pattern);
    return true;
}

bool SubjectAcl::allow_subscribe(const std::string& pattern) {
    if (!SubscriptionRegistry::is_valid_subject(pattern)) {
        return false;
    }
    unrestricted_ = false;
    if (pattern == ">") {
        allow_all_subscribe_ = true;
    }
    subscribe_patterns_.push_back(pattern);
    subscribe_registry_.add_subscription(pattern, "acl", pattern);
    return true;
}

bool SubjectAcl::can_publish(std::string_view subject) const {
    if (unrestricted_ || allow_all_publish_) {
        return true;
    }
    if (subject.empty()) {
        return false;
    }
    return !publish_registry_.match(subject).empty();
}

bool SubjectAcl::can_subscribe(std::string_view subject) const {
    if (unrestricted_ || allow_all_subscribe_) {
        return true;
    }
    if (subject.empty()) {
        return false;
    }
    return !subscribe_registry_.match(subject).empty();
}

bool SubjectAcl::is_unrestricted() const {
    return unrestricted_;
}

void SubjectAcl::clear() {
    unrestricted_ = false;
    allow_all_publish_ = false;
    allow_all_subscribe_ = false;
    publish_patterns_.clear();
    subscribe_patterns_.clear();
    publish_registry_.clear();
    subscribe_registry_.clear();
}

size_t SubjectAcl::publish_rule_count() const {
    return publish_patterns_.size();
}

size_t SubjectAcl::subscribe_rule_count() const {
    return subscribe_patterns_.size();
}

const std::vector<std::string>& SubjectAcl::publish_patterns() const {
    return publish_patterns_;
}

const std::vector<std::string>& SubjectAcl::subscribe_patterns() const {
    return subscribe_patterns_;
}

}
