#pragma once

#include <string>
#include <string_view>
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
    ERR,
    ROUTE_CONNECT,
    RSUB,
    RUNSUB,
    RMSG,
    RPING,
    RPONG
};

std::string to_string(CommandType type);

struct Command {
    CommandType type = CommandType::UNKNOWN;
    std::string subject;
    std::string sid;
    std::string reply_to;
    std::string payload;
    std::string error_message;
    std::string cluster_name;
    std::string node_id;
};

class ProtocolParser {
public:
    ProtocolParser() = default;

    void append(const std::string& data);
    void append(const char* data, size_t length);
    bool next_command(Command& cmd);
    void clear();
    size_t buffered_bytes() const;

    static std::string format_pub(std::string_view subject, std::string_view payload, std::string_view reply_to = "");
    static std::string format_sub(std::string_view subject, std::string_view sid);
    static std::string format_unsub(std::string_view sid);
    static std::string format_msg(std::string_view subject, std::string_view sid, std::string_view payload, std::string_view reply_to = "");
    static std::string format_ping();
    static std::string format_pong();
    static std::string format_ok();
    static std::string format_err(std::string_view reason);
    static std::string format_route_connect(std::string_view cluster_name, std::string_view node_id);
    static std::string format_rsub(std::string_view subject);
    static std::string format_runsub(std::string_view subject);
    static std::string format_rmsg(std::string_view subject, std::string_view payload, std::string_view reply_to = "");
    static std::string format_rping();
    static std::string format_rpong();

private:
    std::string buffer_;

    static std::vector<std::string_view> split_tokens(std::string_view line);
};

}
