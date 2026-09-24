#include "common/protocol.hpp"
#include "common/net.hpp"
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <unistd.h>

namespace p2p {
    namespace {
        void put32(std::string& out, std::uint32_t n) {
            for (int shift = 24; shift >= 0; shift -= 8)
                out.push_back(static_cast<char>((n >> shift) & 255));
        }
        std::uint32_t get32(const std::string& in, std::size_t& pos) {
            if (in.size() - pos < 4) throw std::runtime_error("Truncated field length");
            std::uint32_t n = 0;
            for (int i = 0; i < 4; ++i) n = (n << 8) | static_cast<unsigned char>(in[pos++]);
            return n;
        }
    }

    std::string encode(const Fields& fields) {
        if (fields.size() > 100000) throw std::runtime_error("Too many fields");
        std::string out;
        put32(out, static_cast<std::uint32_t>(fields.size()));
        for (const auto& field : fields) {
            if (field.size() > max_frame || out.size() + 4 + field.size() > max_frame)
                throw std::runtime_error("Frame size limit exceeded");
            put32(out, static_cast<std::uint32_t>(field.size()));
            out += field;
        }
        return out;
    }

    Fields decode(const std::string& bytes) {
        std::size_t pos = 0;
        const auto count = get32(bytes, pos);
        if (count > 100000 || count > (bytes.size() - pos) / 4)
            throw std::runtime_error("Invalid field count");
        Fields fields;
        fields.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            const auto length = get32(bytes, pos);
            if (length > bytes.size() - pos) throw std::runtime_error("Truncated field");
            fields.push_back(bytes.substr(pos, length));
            pos += length;
        }
        if (pos != bytes.size()) throw std::runtime_error("Trailing payload bytes");
        return fields;
    }

    std::uint64_t number(const std::string& text) {
        if (text.empty() || text.size() > 20) throw std::runtime_error("Invalid integer");
        std::uint64_t n = 0;
        for (unsigned char c : text) {
            if (c < '0' || c > '9' || n > (std::numeric_limits<std::uint64_t>::max() - (c - '0')) / 10)
                throw std::runtime_error("Invalid integer");
            n = n * 10 + (c - '0');
        }
        return n;
    }

    std::string random_id() {
        Fd fd(::open("/dev/urandom", O_RDONLY));
        if (fd.get() < 0) throw std::runtime_error("Cannot open system random source");
        unsigned char bytes[32];
        std::size_t done = 0;
        while (done < sizeof(bytes)) {
            const auto n = ::read(fd.get(), bytes + done, sizeof(bytes) - done);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) throw std::runtime_error("Cannot read system random source");
            done += static_cast<std::size_t>(n);
        }
        const char* hex = "0123456789abcdef";
        std::string out;
        for (auto byte : bytes) { out += hex[byte >> 4]; out += hex[byte & 15]; }
        return out;
    }

    Fields Request::fields() const {
        Fields out{id, token, command};
        out.insert(out.end(), args.begin(), args.end());
        return out;
    }
    Request Request::parse(const Fields& fields) {
        if (fields.size() < 3 || fields.size() > 8) throw std::runtime_error("Invalid request fields");
        if (fields[0].empty() || fields[0].size() > 128 || fields[1].size() > 128 || fields[2].size() > 64)
            throw std::runtime_error("Invalid request header");
        for (const auto& field : fields)
            if (field.size() > 256) throw std::runtime_error("Request field too long");
        return {fields[0], fields[1], fields[2], Fields(fields.begin() + 3, fields.end())};
    }
    Fields Response::fields() const {
        Fields out{status, message, mode, token};
        out.insert(out.end(), items.begin(), items.end());
        return out;
    }
    Response Response::parse(const Fields& fields) {
        if (fields.size() < 4) throw std::runtime_error("Invalid response");
        return {fields[0], fields[1], fields[2], fields[3], Fields(fields.begin() + 4, fields.end())};
    }
} // namespace p2p
