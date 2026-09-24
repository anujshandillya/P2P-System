#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace p2p {
    using Fields = std::vector<std::string>;
    constexpr std::size_t max_frame = 16 * 1024 * 1024;

    std::string encode(const Fields& fields);
    Fields decode(const std::string& bytes);
    std::uint64_t number(const std::string& text);
    std::string random_id();

    struct Request {
        std::string id;
        std::string token;
        std::string command;
        Fields args;
        Fields fields() const;
        static Request parse(const Fields& fields);
    };

    struct Response {
        std::string status = "OK";
        std::string message;
        std::string mode = "local";
        std::string token;
        Fields items;
        Fields fields() const;
        static Response parse(const Fields& fields);
    };
} // namespace p2p
