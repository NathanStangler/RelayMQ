#include "relaymq/protocol.hpp"

#include <sstream>
#include <algorithm>
#include <cctype>

namespace relaymq {

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

std::vector<std::string> ProtocolParser::split_tokens(const std::string& line) {
    std::vector<std::string> tokens;
    std::istringstream stream(line);
    std::string token;
    while (stream >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

bool ProtocolParser::next_command(Command& out_cmd) {
    while (true) {
        size_t line_end = buffer_.find("\r\n");
        if (line_end == std::string::npos) {
            return false;
        }

        std::string line = buffer_.substr(0, line_end);
        std::vector<std::string> tokens = split_tokens(line);

        if (tokens.empty()) {
            buffer_.erase(0, line_end + 2);
            continue;
        }

        std::string verb = tokens[0];
        for (char& c : verb) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }

        if (verb == "PING") {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::PING;
            return true;
        }

        if (verb == "PONG") {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::PONG;
            return true;
        }

        if (verb == "OK") {
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::OK;
            return true;
        }

        if (verb == "ERR") {
            std::string err_msg;
            if (line.size() > verb.size()) {
                err_msg = line.substr(verb.size() + 1);
            }
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::ERR;
            out_cmd.error_message = err_msg;
            return true;
        }

        if (verb == "SUB") {
            if (tokens.size() < 3) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid SUB syntax: expected SUB <subject> <sid>";
                return true;
            }
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::SUB;
            out_cmd.subject = tokens[1];
            out_cmd.sid = tokens[2];
            return true;
        }

        if (verb == "UNSUB") {
            if (tokens.size() < 2) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid UNSUB syntax: expected UNSUB <sid>";
                return true;
            }
            buffer_.erase(0, line_end + 2);
            out_cmd = Command{};
            out_cmd.type = CommandType::UNSUB;
            out_cmd.sid = tokens[1];
            return true;
        }

        if (verb == "PUB") {
            if (tokens.size() < 3) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid PUB syntax: expected PUB <subject> [reply-to] <bytes>";
                return true;
            }

            std::string subject = tokens[1];
            std::string reply_to;
            size_t payload_len = 0;

            try {
                if (tokens.size() == 3) {
                    payload_len = std::stoull(tokens[2]);
                } else {
                    reply_to = tokens[2];
                    payload_len = std::stoull(tokens[3]);
                }
            } catch (const std::exception&) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid byte length in PUB command";
                return true;
            }

            size_t total_required = line_end + payload_len + 4;
            if (buffer_.size() < total_required) {
                return false;
            }

            std::string payload = buffer_.substr(line_end + 2, payload_len);
            buffer_.erase(0, total_required);

            out_cmd = Command{};
            out_cmd.type = CommandType::PUB;
            out_cmd.subject = subject;
            out_cmd.reply_to = reply_to;
            out_cmd.payload = payload;
            return true;
        }

        if (verb == "MSG") {
            if (tokens.size() < 4) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid MSG syntax: expected MSG <subject> <sid> [reply-to] <bytes>";
                return true;
            }

            std::string subject = tokens[1];
            std::string sid = tokens[2];
            std::string reply_to;
            size_t payload_len = 0;

            try {
                if (tokens.size() == 4) {
                    payload_len = std::stoull(tokens[3]);
                } else {
                    reply_to = tokens[3];
                    payload_len = std::stoull(tokens[4]);
                }
            } catch (const std::exception&) {
                buffer_.erase(0, line_end + 2);
                out_cmd = Command{};
                out_cmd.type = CommandType::ERR;
                out_cmd.error_message = "Invalid byte length in MSG command";
                return true;
            }

            size_t total_required = line_end + payload_len + 4;
            if (buffer_.size() < total_required) {
                return false;
            }

            std::string payload = buffer_.substr(line_end + 2, payload_len);
            buffer_.erase(0, total_required);

            out_cmd = Command{};
            out_cmd.type = CommandType::MSG;
            out_cmd.subject = subject;
            out_cmd.sid = sid;
            out_cmd.reply_to = reply_to;
            out_cmd.payload = payload;
            return true;
        }

        buffer_.erase(0, line_end + 2);
        out_cmd = Command{};
        out_cmd.type = CommandType::ERR;
        out_cmd.error_message = "Unknown command: " + tokens[0];
        return true;
    }
}

std::string ProtocolParser::format_pub(const std::string& subject, const std::string& payload, const std::string& reply_to) {
    std::ostringstream ss;
    if (reply_to.empty()) {
        ss << "PUB " << subject << " " << payload.size() << "\r\n" << payload << "\r\n";
    } else {
        ss << "PUB " << subject << " " << reply_to << " " << payload.size() << "\r\n" << payload << "\r\n";
    }
    return ss.str();
}

std::string ProtocolParser::format_sub(const std::string& subject, const std::string& sid) {
    return "SUB " + subject + " " + sid + "\r\n";
}

std::string ProtocolParser::format_unsub(const std::string& sid) {
    return "UNSUB " + sid + "\r\n";
}

std::string ProtocolParser::format_msg(const std::string& subject, const std::string& sid, const std::string& payload, const std::string& reply_to) {
    std::ostringstream ss;
    if (reply_to.empty()) {
        ss << "MSG " << subject << " " << sid << " " << payload.size() << "\r\n" << payload << "\r\n";
    } else {
        ss << "MSG " << subject << " " << sid << " " << reply_to << " " << payload.size() << "\r\n" << payload << "\r\n";
    }
    return ss.str();
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

std::string ProtocolParser::format_err(const std::string& reason) {
    return "ERR " + reason + "\r\n";
}

}
