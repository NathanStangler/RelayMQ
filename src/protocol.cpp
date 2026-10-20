#include "relaymq/protocol.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace relaymq {

namespace {

bool equals_ignore_case(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); i++) {
        if (std::toupper(static_cast<unsigned char>(a[i])) != std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

bool parse_uint64(std::string_view sv, size_t& value) {
    if (sv.empty()) {
        return false;
    }
    std::from_chars_result result = std::from_chars(sv.data(), sv.data() + sv.size(), value);
    return result.ec == std::errc() && result.ptr == sv.data() + sv.size();
}

}

std::string to_string(CommandType type) {
    switch (type) {
        case CommandType::PUB: return "PUB";
        case CommandType::SUB: return "SUB";
        case CommandType::UNSUB: return "UNSUB";
        case CommandType::MSG: return "MSG";
        case CommandType::PING: return "PING";
        case CommandType::PONG: return "PONG";
        case CommandType::OK: return "OK";
        case CommandType::ERR: return "ERR";
        case CommandType::ROUTE_CONNECT: return "ROUTE_CONNECT";
        case CommandType::RSUB: return "RSUB";
        case CommandType::RUNSUB: return "RUNSUB";
        case CommandType::RMSG: return "RMSG";
        case CommandType::RPING: return "RPING";
        case CommandType::RPONG: return "RPONG";
        case CommandType::AUTH: return "AUTH";
        case CommandType::UNKNOWN: return "UNKNOWN";
        default: return "UNKNOWN";
    }
}

void ProtocolParser::append(const std::string& data) {
    buffer_.append(data);
}

void ProtocolParser::append(const char* data, size_t length) {
    buffer_.append(data, length);
}

void ProtocolParser::clear() {
    buffer_.clear();
}

size_t ProtocolParser::buffered_bytes() const {
    return buffer_.size();
}

std::vector<std::string_view> ProtocolParser::split_tokens(std::string_view line) {
    std::vector<std::string_view> tokens;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
            i++;
        }
        if (i >= line.size()) {
            break;
        }
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
            i++;
        }
        tokens.push_back(line.substr(start, i - start));
    }
    return tokens;
}

bool ProtocolParser::parse_simple_command(CommandType type, size_t line_end, Command& cmd) {
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = type;
    return true;
}

bool ProtocolParser::parse_single_subject_command(CommandType type, const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    std::string action_name = to_string(type);
    if (tokens.size() < 2) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid " + action_name + " syntax: expected " + action_name + " <subject>";
        return true;
    }
    std::string subject(tokens[1]);
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = type;
    cmd.subject = std::move(subject);
    return true;
}

bool ProtocolParser::parse_payload_command(CommandType type, const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    std::string action_name = to_string(type);
    if (tokens.size() < 3) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid " + action_name + " syntax: expected " + action_name + " <subject> [reply-to] <bytes>";
        return true;
    }

    std::string_view subject = tokens[1];
    std::string_view reply_to;
    size_t payload_len = 0;

    if (tokens.size() == 3) {
        if (!parse_uint64(tokens[2], payload_len)) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid byte length in " + action_name + " command";
            return true;
        }
    } else {
        reply_to = tokens[2];
        if (!parse_uint64(tokens[3], payload_len)) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid byte length in " + action_name + " command";
            return true;
        }
    }

    size_t total_required = line_end + payload_len + 4;
    if (buffer_.size() < total_required) {
        return false;
    }

    std::string s_subject(subject);
    std::string s_reply_to(reply_to);
    std::string payload = buffer_.substr(line_end + 2, payload_len);
    buffer_.erase(0, total_required);

    cmd = Command{};
    cmd.type = type;
    cmd.subject = std::move(s_subject);
    cmd.reply_to = std::move(s_reply_to);
    cmd.payload = std::move(payload);
    return true;
}

bool ProtocolParser::parse_sub(const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    if (tokens.size() < 3) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid SUB syntax: expected SUB <subject> <sid>";
        return true;
    }
    std::string sub_subject(tokens[1]);
    std::string sub_sid(tokens[2]);
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::SUB;
    cmd.subject = std::move(sub_subject);
    cmd.sid = std::move(sub_sid);
    return true;
}

bool ProtocolParser::parse_unsub(const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    if (tokens.size() < 2) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid UNSUB syntax: expected UNSUB <sid>";
        return true;
    }
    std::string unsub_sid(tokens[1]);
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::UNSUB;
    cmd.sid = std::move(unsub_sid);
    return true;
}

bool ProtocolParser::parse_msg(const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    if (tokens.size() < 4) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid MSG syntax: expected MSG <subject> <sid> [reply-to] <bytes>";
        return true;
    }

    std::string_view subject = tokens[1];
    std::string_view sid = tokens[2];
    std::string_view reply_to;
    size_t payload_len = 0;

    if (tokens.size() == 4) {
        if (!parse_uint64(tokens[3], payload_len)) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid byte length in MSG command";
            return true;
        }
    } else {
        reply_to = tokens[3];
        if (!parse_uint64(tokens[4], payload_len)) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid byte length in MSG command";
            return true;
        }
    }

    size_t total_required = line_end + payload_len + 4;
    if (buffer_.size() < total_required) {
        return false;
    }

    std::string msg_subject(subject);
    std::string msg_sid(sid);
    std::string msg_reply_to(reply_to);
    std::string payload = buffer_.substr(line_end + 2, payload_len);
    buffer_.erase(0, total_required);

    cmd = Command{};
    cmd.type = CommandType::MSG;
    cmd.subject = std::move(msg_subject);
    cmd.sid = std::move(msg_sid);
    cmd.reply_to = std::move(msg_reply_to);
    cmd.payload = std::move(payload);
    return true;
}

bool ProtocolParser::parse_route_connect(const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    if (tokens.size() < 3) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid ROUTE_CONNECT syntax: expected ROUTE_CONNECT <cluster_name> <node_id> [cluster_token]";
        return true;
    }
    std::string cname(tokens[1]);
    std::string nid(tokens[2]);
    std::string token;
    if (tokens.size() >= 4) {
        token = std::string(tokens[3]);
    }
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::ROUTE_CONNECT;
    cmd.cluster_name = std::move(cname);
    cmd.node_id = std::move(nid);
    cmd.auth_token = std::move(token);
    return true;
}

bool ProtocolParser::parse_auth(const std::vector<std::string_view>& tokens, size_t line_end, Command& cmd) {
    if (tokens.size() < 2) {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Invalid AUTH syntax: expected AUTH <token>";
        return true;
    }
    std::string token(tokens[1]);
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::AUTH;
    cmd.auth_token = std::move(token);
    return true;
}

bool ProtocolParser::parse_err(std::string_view line, std::string_view action, size_t line_end, Command& cmd) {
    std::string err_msg;
    if (line.size() > action.size()) {
        std::string_view msg_view = line.substr(action.size() + 1);
        err_msg = std::string(msg_view);
    }
    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::ERR;
    cmd.error_message = err_msg;
    return true;
}

bool ProtocolParser::next_command(Command& cmd) {
    while (true) {
        size_t line_end = buffer_.find("\r\n");
        if (line_end == std::string::npos) {
            return false;
        }

        std::string_view line = std::string_view(buffer_).substr(0, line_end);
        std::vector<std::string_view> tokens = split_tokens(line);

        if (tokens.empty()) {
            buffer_.erase(0, line_end + 2);
            continue;
        }

        std::string_view action = tokens[0];

        if (equals_ignore_case(action, "PUB")) {
            return parse_payload_command(CommandType::PUB, tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "RMSG")) {
            return parse_payload_command(CommandType::RMSG, tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "SUB")) {
            return parse_sub(tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "UNSUB")) {
            return parse_unsub(tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "MSG")) {
            return parse_msg(tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "ROUTE_CONNECT")) {
            return parse_route_connect(tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "RSUB")) {
            return parse_single_subject_command(CommandType::RSUB, tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "RUNSUB")) {
            return parse_single_subject_command(CommandType::RUNSUB, tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "PING")) {
            return parse_simple_command(CommandType::PING, line_end, cmd);
        }
        if (equals_ignore_case(action, "PONG")) {
            return parse_simple_command(CommandType::PONG, line_end, cmd);
        }
        if (equals_ignore_case(action, "RPING")) {
            return parse_simple_command(CommandType::RPING, line_end, cmd);
        }
        if (equals_ignore_case(action, "RPONG")) {
            return parse_simple_command(CommandType::RPONG, line_end, cmd);
        }
        if (equals_ignore_case(action, "OK")) {
            return parse_simple_command(CommandType::OK, line_end, cmd);
        }
        if (equals_ignore_case(action, "AUTH")) {
            return parse_auth(tokens, line_end, cmd);
        }
        if (equals_ignore_case(action, "ERR")) {
            return parse_err(line, action, line_end, cmd);
        }

        std::string unknown_action(tokens[0]);
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = "Unknown command: " + unknown_action;
        return true;
    }
}

std::string ProtocolParser::format_pub(std::string_view subject, std::string_view payload, std::string_view reply_to) {
    std::string out;
    std::string size_str = std::to_string(payload.size());
    size_t total_len = subject.size() + size_str.size() + payload.size() + 9;
    if (!reply_to.empty()) {
        total_len += reply_to.size() + 1;
    }
    out.reserve(total_len);

    out.append("PUB ");
    out.append(subject);
    out.append(" ");
    if (!reply_to.empty()) {
        out.append(reply_to);
        out.append(" ");
    }
    out.append(size_str);
    out.append("\r\n");
    out.append(payload);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_sub(std::string_view subject, std::string_view sid) {
    std::string out;
    out.reserve(subject.size() + sid.size() + 7);
    out.append("SUB ");
    out.append(subject);
    out.append(" ");
    out.append(sid);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_unsub(std::string_view sid) {
    std::string out;
    out.reserve(sid.size() + 8);
    out.append("UNSUB ");
    out.append(sid);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_msg(std::string_view subject, std::string_view sid, std::string_view payload, std::string_view reply_to) {
    std::string out;
    std::string size_str = std::to_string(payload.size());
    size_t total_len = subject.size() + sid.size() + size_str.size() + payload.size() + 10;
    if (!reply_to.empty()) {
        total_len += reply_to.size() + 1;
    }
    out.reserve(total_len);

    out.append("MSG ");
    out.append(subject);
    out.append(" ");
    out.append(sid);
    out.append(" ");
    if (!reply_to.empty()) {
        out.append(reply_to);
        out.append(" ");
    }
    out.append(size_str);
    out.append("\r\n");
    out.append(payload);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_ping() {
    return "PING\r\n";
}

std::string ProtocolParser::format_pong() {
    return "PONG\r\n";
}

std::string ProtocolParser::format_ok() {
    return "OK\r\n";
}

std::string ProtocolParser::format_err(std::string_view reason) {
    std::string out;
    out.reserve(reason.size() + 6);
    out.append("ERR ");
    out.append(reason);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_route_connect(std::string_view cluster_name, std::string_view node_id, std::string_view auth_token) {
    std::string out;
    size_t total_len = cluster_name.size() + node_id.size() + 17;
    if (!auth_token.empty()) {
        total_len += auth_token.size() + 1;
    }
    out.reserve(total_len);
    out.append("ROUTE_CONNECT ");
    out.append(cluster_name);
    out.append(" ");
    out.append(node_id);
    if (!auth_token.empty()) {
        out.append(" ");
        out.append(auth_token);
    }
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_auth(std::string_view token) {
    std::string out;
    out.reserve(token.size() + 7);
    out.append("AUTH ");
    out.append(token);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_rsub(std::string_view subject) {
    std::string out;
    out.reserve(subject.size() + 7);
    out.append("RSUB ");
    out.append(subject);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_runsub(std::string_view subject) {
    std::string out;
    out.reserve(subject.size() + 9);
    out.append("RUNSUB ");
    out.append(subject);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_rmsg(std::string_view subject, std::string_view payload, std::string_view reply_to) {
    std::string out;
    std::string size_str = std::to_string(payload.size());
    size_t total_len = subject.size() + size_str.size() + payload.size() + 10;
    if (!reply_to.empty()) {
        total_len += reply_to.size() + 1;
    }
    out.reserve(total_len);

    out.append("RMSG ");
    out.append(subject);
    out.append(" ");
    if (!reply_to.empty()) {
        out.append(reply_to);
        out.append(" ");
    }
    out.append(size_str);
    out.append("\r\n");
    out.append(payload);
    out.append("\r\n");
    return out;
}

std::string ProtocolParser::format_rping() {
    return "RPING\r\n";
}

std::string ProtocolParser::format_rpong() {
    return "RPONG\r\n";
}

}
