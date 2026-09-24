#pragma once

#include "common/net.hpp"
#include <optional>

namespace p2p {
Fields split_command(const std::string& line);
class Client {
public:
    Client(std::string endpoint, std::array<Endpoint, 2> trackers, int preferred);
    void run();
private:
    Response dispatch(const Request& request);
    void print(const Response& response);
    std::string endpoint_;
    std::array<Endpoint, 2> trackers_;
    int preferred_;
    std::string token_;
    std::optional<Request> pending_;
};
} // namespace p2p
