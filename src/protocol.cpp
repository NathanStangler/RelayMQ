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

bool ProtocolParser::next_command(Command& out_cmd) {
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

        std::string_view verb = tokens[0];

        if (equals_ignore_case(verb, "PING")) {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::PING;
            return true;
        }

        if (equals_ignore_case(verb, "PONG")) {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::PONG;
            return true;
        }

        if (equals_ignore_case(verb, "OK")) {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::OK;
            return true;
        }

        if (equals_ignore_case(verb, "ERR")) {
            std::string err_msg;
            if (line.size() > verb.size()) {
                std::string_view msg_view = line.substr(verb.size() + 1);
                err_msg = std::string(msg_view);
            }
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::ERR;
            out_cmd.error_message = err_msg;
            return true;
        }

        if (equals_ignore_case(verb, "SUB")) {
            if (tokens.size() < 3) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid SUB syntax: expected SUB <subject> <sid>";
                return true;
            }
            std::string sub_subject(tokens[1]);
            std::string sub_sid(tokens[2]);
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::SUB;
            out_cmd.subject = std::move(sub_subject);
            out_cmd.sid = std::move(sub_sid);
            return true;
        }

        if (equals_ignore_case(verb, "UNSUB")) {
            if (tokens.size() < 2) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid UNSUB syntax: expected UNSUB <sid>";
                return true;
            }
            std::string unsub_sid(tokens[1]);
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::UNSUB;
            out_cmd.sid = std::move(unsub_sid);
            return true;
        }

        if (equals_ignore_case(verb, "PUB")) {
            if (tokens.size() < 3) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid PUB syntax: expected PUB <subject> [reply-to] <bytes>";
                return true;
            }

            std::string_view subject = tokens[1];
            std::string_view reply_to;
            size_t payload_len = 0;

            if (tokens.size() == 3) {
                if (!parse_uint64(tokens[2], payload_len)) {
                    buffer_.erase(0, line_end + 2);
                    out_cmd = Command{};
                    out_cmd.type = CommandType::ERR;
                    out_cmd.error_message = "Invalid byte length in PUB command";
                    return true;
                }
            } else {
                reply_to = tokens[2];
                if (!parse_uint64(tokens[3], payload_len)) {
                    buffer_.erase(0, line_end + 2);
                    out_cmd = Command{};
                    out_cmd.type = CommandType::ERR;
                    out_cmd.error_message = "Invalid byte length in PUB command";
                    return true;
                }
            }

            size_t total_required = line_end + payload_len + 4;
            if (buffer_.size() < total_required) {
                return false;
            }

            std::string pub_subject(subject);
            std::string pub_reply_to(reply_to);
            std::string payload = buffer_.substr(line_end + 2, payload_len);
            buffer_.erase(0, total_required);

            out_cmd = Command{};
            out_cmd.type = CommandType::PUB;
            out_cmd.subject = std::move(pub_subject);
            out_cmd.reply_to = std::move(pub_reply_to);
            out_cmd.payload = std::move(payload);
            return true;
        }

        if (equals_ignore_case(verb, "MSG")) {
            if (tokens.size() < 4) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid MSG syntax: expected MSG <subject> <sid> [reply-to] <bytes>";
                return true;
            }

            std::string_view subject = tokens[1];
            std::string_view sid = tokens[2];
            std::string_view reply_to;
            size_t payload_len = 0;

            if (tokens.size() == 4) {
                if (!parse_uint64(tokens[3], payload_len)) {
                    buffer_.erase(0, line_end + 2);
                    out_cmd = Command{};
                    out_cmd.type = CommandType::ERR;
                    out_cmd.error_message = "Invalid byte length in MSG command";
                    return true;
                }
            } else {
                reply_to = tokens[3];
                if (!parse_uint64(tokens[4], payload_len)) {
                    buffer_.erase(0, line_end + 2);
                    out_cmd = Command{};
                    out_cmd.type = CommandType::ERR;
                    out_cmd.error_message = "Invalid byte length in MSG command";
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

            out_cmd = Command{};
            out_cmd.type = CommandType::MSG;
            out_cmd.subject = std::move(msg_subject);
            out_cmd.sid = std::move(msg_sid);
            out_cmd.reply_to = std::move(msg_reply_to);
            out_cmd.payload = std::move(payload);
            return true;
        }

        std::string unknown_verb(tokens[0]);
        buffer_.erase(0, line_end + 2);
        out_cmd = Command{};
        out_cmd.type = CommandType::ERR;
        out_cmd.error_message = "Unknown command: " + unknown_verb;
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

}
