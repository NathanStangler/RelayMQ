#pragma once

#include <string>
#include <string_view>
#include <cstdint>

namespace relaymq {

struct Endpoint {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;

    Endpoint() = default;
    Endpoint(std::string h, uint16_t p);

    static bool parse(std::string_view str, Endpoint& endpoint);
    std::string to_string() const;
};

}
