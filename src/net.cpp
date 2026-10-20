#include "relaymq/net.hpp"

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <cerrno>
#include <thread>

namespace relaymq::net {

bool set_nonblocking(int fd) {
    if (fd < 0) {
        return false;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool set_tcp_nodelay(int fd, bool enable) {
    if (fd < 0) {
        return false;
    }
    int opt = enable ? 1 : 0;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0;
}

bool set_reuseaddr(int fd, bool enable) {
    if (fd < 0) {
        return false;
    }
    int opt = enable ? 1 : 0;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0;
}

int create_tcp_listener(uint16_t listen_port, int backlog, uint16_t* port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    set_reuseaddr(fd, true);
    set_nonblocking(fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(listen_port);

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, backlog) < 0) {
        close(fd);
        return -1;
    }

    if (port) {
        socklen_t len = sizeof(addr);
        if (getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0) {
            *port = ntohs(addr.sin_port);
        } else {
            *port = listen_port;
        }
    }

    return fd;
}

int connect_tcp(const std::string& host, uint16_t port, bool nonblocking) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    set_tcp_nodelay(fd, true);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (nonblocking) {
        set_nonblocking(fd);
    }

    return fd;
}

void close_socket(int& fd) {
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
        fd = -1;
    }
}

ssize_t send_all(int fd, std::string_view data, int max_yields) {
    if (fd < 0) {
        return -1;
    }

    size_t total_sent = 0;
    int yields = 0;

    while (total_sent < data.size()) {
        ssize_t sent = ::send(fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                yields++;
                if (yields > max_yields) {
                    break;
                }
                std::this_thread::yield();
                continue;
            }
            return -1;
        }
        total_sent += static_cast<size_t>(sent);
        yields = 0;
    }

    return static_cast<ssize_t>(total_sent);
}

}
