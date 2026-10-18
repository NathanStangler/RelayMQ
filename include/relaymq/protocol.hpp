#pragma once

#include <string>
#include <vector>

namespace relaymq {

enum class CommandType {
    UNKNOWN = 0,
    PUB,
    SUB,
    UNSUB,
    MSG,
    PING,
    PONG,
    OK,
    ERR
};

std::string to_string(CommandType type);

struct Command {
    CommandType type = CommandType::UNKNOWN;
    std::string subject;
    std::string sid;
    std::string reply_to;
    std::string payload;
    std::string error_message;
};

class ProtocolParser {
public:
    ProtocolParser() = default;

    void append(const std::string& data);
    void append(const char* data, size_t length);
    bool next_command(Command& cmd);
    void clear();
    size_t buffered_bytes() const;

    static std::string format_pub(const std::string& subject, const std::string& payload, const std::string& reply_to = "");
    static std::string format_sub(const std::string& subject, const std::string& sid);
    static std::string format_unsub(const std::string& sid);
    static std::string format_msg(const std::string& subject, const std::string& sid, const std::string& payload, const std::string& reply_to = "");
    static std::string format_ping();
    static std::string format_pong();
    static std::string format_ok();
    static std::string format_err(const std::string& reason);

private:
    std::string buffer_;

    static std::vector<std::string> split_tokens(const std::string& line);
};

}
