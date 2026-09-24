#include "common/net.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace p2p {
Fd::~Fd() { if (value_ >= 0) ::close(value_); }
Fd::Fd(Fd&& other) noexcept : value_(other.value_) { other.value_ = -1; }
Fd& Fd::operator=(Fd&& other) noexcept {
    if (this != &other) {
        if (value_ >= 0) ::close(value_);
        value_ = other.value_;
        other.value_ = -1;
    }
    return *this;
}
namespace {
using Clock = std::chrono::steady_clock;
void wait_for(int fd, short events, Clock::time_point deadline) {
    for (;;) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0) throw std::runtime_error("Network timeout");
        pollfd p{fd, events, 0};
        const int n = ::poll(&p, 1, static_cast<int>(remaining));
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) throw std::runtime_error("poll failed");
        if (n == 0) throw std::runtime_error("Network timeout");
        if (p.revents & POLLNVAL) throw std::runtime_error("Invalid socket");
        return; // recv/send/getsockopt will distinguish hangup and error.
    }
}
void transfer(int fd, char* data, std::size_t length, bool sending, Clock::time_point deadline) {
    std::size_t done = 0;
    while (done < length) {
        wait_for(fd, sending ? POLLOUT : POLLIN, deadline);
        const auto n = sending ? ::send(fd, data + done, length - done, MSG_DONTWAIT)
                               : ::recv(fd, data + done, length - done, MSG_DONTWAIT);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (n <= 0) throw std::runtime_error(n == 0 ? "Connection closed" : "Socket I/O failed");
        done += static_cast<std::size_t>(n);
    }
}
sockaddr_in address(const Endpoint& ep) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ep.port);
    if (::inet_pton(AF_INET, ep.ip.c_str(), &addr.sin_addr) != 1)
        throw std::runtime_error("Expected a numeric IPv4 address");
    return addr;
}
}

Endpoint parse_endpoint(const std::string& value) {
    const auto pos = value.find(':');
    if (pos == std::string::npos) throw std::runtime_error("Expected IP:PORT");
    const auto port = number(value.substr(pos + 1));
    if (port == 0 || port > 65535) throw std::runtime_error("Port must be 1..65535");
    Endpoint ep{value.substr(0, pos), static_cast<unsigned short>(port)};
    (void)address(ep);
    return ep;
}

std::array<Endpoint, 2> read_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open tracker configuration: " + path);
    std::vector<Endpoint> endpoints;
    std::string line;
    while (std::getline(in, line)) {
        line = line.substr(0, line.find('#'));
        std::istringstream stream(line);
        std::string ip, port, extra;
        if (!(stream >> ip)) continue;
        if (!(stream >> port) || (stream >> extra)) throw std::runtime_error("Configuration requires IP PORT per line");
        endpoints.push_back(parse_endpoint(ip + ":" + port));
    }
    if (endpoints.size() != 2) throw std::runtime_error("Configuration requires exactly two trackers");
    if (endpoints[0].ip == endpoints[1].ip && endpoints[0].port == endpoints[1].port)
        throw std::runtime_error("Tracker endpoints must differ");
    return {endpoints[0], endpoints[1]};
}

Fd listen_on(const Endpoint& endpoint) {
    Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (fd.get() < 0) throw std::runtime_error("socket failed");
    int yes = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
        throw std::runtime_error("setsockopt failed");
    const auto addr = address(endpoint);
    if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind failed: " + std::string(std::strerror(errno)));
    if (::listen(fd.get(), 64) < 0) throw std::runtime_error("listen failed");
    return fd;
}

Fd connect_to(const Endpoint& endpoint, int timeout_ms) {
    Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (fd.get() < 0) throw std::runtime_error("socket failed");
    if (::fcntl(fd.get(), F_SETFL, O_NONBLOCK) < 0) throw std::runtime_error("fcntl failed");
    const auto addr = address(endpoint);
    if (::connect(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) throw std::runtime_error("Tracker unavailable");
        wait_for(fd.get(), POLLOUT, Clock::now() + std::chrono::milliseconds(timeout_ms));
        int error = 0;
        socklen_t length = sizeof(error);
        if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &length) < 0 || error != 0)
            throw std::runtime_error("Tracker unavailable");
    }
    return fd;
}

void send_frame(int fd, const Fields& fields, int timeout_ms) {
    auto payload = encode(fields);
    const auto length = static_cast<std::uint32_t>(payload.size());
    std::string frame = "P2P1";
    for (int shift = 24; shift >= 0; shift -= 8) frame += static_cast<char>((length >> shift) & 255);
    frame += payload;
    transfer(fd, frame.data(), frame.size(), true, Clock::now() + std::chrono::milliseconds(timeout_ms));
}
Fields receive_frame(int fd, int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    char header[8];
    transfer(fd, header, sizeof(header), false, deadline);
    if (std::memcmp(header, "P2P1", 4) != 0) throw std::runtime_error("Unknown protocol/version");
    std::uint32_t length = 0;
    for (int i = 4; i < 8; ++i) length = (length << 8) | static_cast<unsigned char>(header[i]);
    if (length < 4 || length > max_frame) throw std::runtime_error("Invalid frame length");
    std::string payload(length, '\0');
    transfer(fd, payload.data(), payload.size(), false, deadline);
    return decode(payload);
}
Fields rpc(const Endpoint& endpoint, const Fields& fields, int timeout_ms) {
    auto fd = connect_to(endpoint, timeout_ms);
    send_frame(fd.get(), fields, timeout_ms);
    return receive_frame(fd.get(), timeout_ms);
}
void ignore_sigpipe() { std::signal(SIGPIPE, SIG_IGN); }
} // namespace p2p
