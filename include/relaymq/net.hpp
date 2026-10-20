#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <sys/types.h>

namespace relaymq::net {

bool set_nonblocking(int fd);
bool set_tcp_nodelay(int fd, bool enable = true);
bool set_reuseaddr(int fd, bool enable = true);
int create_tcp_listener(uint16_t listen_port, int backlog = 128, uint16_t* port = nullptr);
int connect_tcp(const std::string& host, uint16_t port, bool nonblocking = false);
void close_socket(int& fd);
ssize_t send_all(int fd, std::string_view data, int max_yields = 1000);

}
