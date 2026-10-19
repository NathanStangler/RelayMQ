#pragma once

#include "relaymq/protocol.hpp"
#include <string>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <cstdint>

namespace relaymq {

struct Message {
    std::string subject;
    std::string sid;
    std::string reply_to;
    std::string payload;
};

using MessageCallback = std::function<void(const Message&)>;
using DisconnectCallback = std::function<void()>;

class Client {
public:
    Client();
    ~Client();

    bool connect(const std::string& host = "127.0.0.1", uint16_t port = 9000);
    void disconnect();
    bool is_connected() const;
    void set_disconnect_callback(DisconnectCallback callback);
    bool publish(const std::string& subject, const std::string& payload, const std::string& reply_to = "");
    std::string subscribe(const std::string& subject, MessageCallback callback);
    bool unsubscribe(const std::string& sid);
    int64_t ping(int timeout_ms = 2000);

private:
    int socket_fd_;
    std::atomic<bool> is_connected_;
    ProtocolParser parser_;
    std::thread receive_thread_;
    std::mutex send_mutex_;
    std::mutex sub_mutex_;
    std::atomic<uint64_t> next_sid_;
    std::unordered_map<std::string, MessageCallback> subscriptions_;
    DisconnectCallback disconnect_callback_;
    std::mutex pong_mutex_;
    std::condition_variable pong_cv_;
    std::atomic<bool> awaiting_pong_;

    bool send_raw(const std::string& data);
    void receive_loop();
};

}
