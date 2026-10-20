#pragma once

#include "relaymq/net.hpp"
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

namespace relaymq::test {

class TestSocket {
public:
    int fd = -1;
    std::string buffer;

    TestSocket() = default;

    ~TestSocket() {
        close();
    }

    TestSocket(const TestSocket&) = delete;
    TestSocket& operator=(const TestSocket&) = delete;

    bool connect(uint16_t port, const std::string& host = "127.0.0.1") {
        close();
        buffer.clear();
        fd = net::connect_tcp(host, port, false);
        return fd >= 0;
    }

    void close() {
        if (fd >= 0) {
            net::close_socket(fd);
            fd = -1;
        }
    }

    bool is_connected() const {
        return fd >= 0;
    }

    bool send(std::string_view data) {
        if (fd < 0) {
            return false;
        }
        ssize_t sent = net::send_all(fd, data);
        return sent == static_cast<ssize_t>(data.size());
    }

    std::string read_line(int timeout_ms = 1000) {
        while (!buffer.contains("\r\n")) {
            if (fd < 0) {
                break;
            }
            struct pollfd pfd{.fd = fd, .events = POLLIN, .revents = 0};
            int ret = poll(&pfd, 1, timeout_ms);
            if (ret <= 0) {
                break;
            }
            char chunk[512];
            ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) {
                break;
            }
            buffer.append(chunk, static_cast<size_t>(n));
        }
        size_t pos = buffer.find("\r\n");
        if (pos == std::string::npos) {
            return "";
        }
        std::string line = buffer.substr(0, pos + 2);
        buffer.erase(0, pos + 2);
        return line;
    }
};

}
