#include "relaymq/acl.hpp"

namespace relaymq {

static std::vector<std::string> split_comma(std::string_view str) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t end = str.find(',');
    while (end != std::string_view::npos) {
        if (end > start) {
            result.emplace_back(str.substr(start, end - start));
        }
        start = end + 1;
        end = str.find(',', start);
    }
    if (start < str.size()) {
        result.emplace_back(str.substr(start));
    }
    return result;
}

bool TokenRule::parse(std::string_view rule_str, TokenRule& rule) {
    size_t first_colon = rule_str.find(':');
    if (first_colon == std::string_view::npos) {
        return false;
    }
    std::string_view token_part = rule_str.substr(0, first_colon);
    if (token_part.empty()) {
        return false;
    }

    size_t second_colon = rule_str.find(':', first_colon + 1);
    if (second_colon == std::string_view::npos) {
        return false;
    }

    std::string_view pub_part = rule_str.substr(first_colon + 1, second_colon - (first_colon + 1));
    std::string_view sub_part = rule_str.substr(second_colon + 1);

    rule.token = std::string(token_part);
    rule.allow_publish.clear();
    rule.allow_subscribe.clear();

    if (!pub_part.empty() && pub_part != "none") {
        rule.allow_publish = split_comma(pub_part);
    }
    if (!sub_part.empty() && sub_part != "none") {
        rule.allow_subscribe = split_comma(sub_part);
    }

    return true;
}

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
