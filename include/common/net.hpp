#pragma once

#include "common/protocol.hpp"
#include <array>
#include <string>

namespace p2p {
    class Fd {
    public:
        explicit Fd(int value = -1) : value_(value) {}
        ~Fd();
        Fd(const Fd&) = delete;
        Fd& operator=(const Fd&) = delete;
        Fd(Fd&& other) noexcept;
        Fd& operator=(Fd&& other) noexcept;
        int get() const { return value_; }
    private:
        int value_;
    };

    struct Endpoint {
        std::string ip;
        unsigned short port;
    };
    
    Endpoint parse_endpoint(const std::string& value);
    std::array<Endpoint, 2> read_config(const std::string& path);
    Fd listen_on(const Endpoint& endpoint);
    Fd connect_to(const Endpoint& endpoint, int timeout_ms = 2000);
    void send_frame(int fd, const Fields& fields, int timeout_ms = 3000);
    Fields receive_frame(int fd, int timeout_ms = 3000);
    Fields rpc(const Endpoint& endpoint, const Fields& fields, int timeout_ms = 3000);
    void ignore_sigpipe();
} // namespace p2p
