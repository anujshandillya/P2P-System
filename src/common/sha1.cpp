#include "common/sha1.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
namespace p2p {
    namespace {
        std::uint32_t rotate(std::uint32_t x, unsigned n) { return (x << n) | (x >> (32 - n)); }
    }
    void Sha1::transform(const std::uint8_t* block) {
        std::uint32_t w[80];
        for (unsigned i = 0; i < 16; ++i)
            w[i] = (std::uint32_t(block[4*i]) << 24) | (std::uint32_t(block[4*i+1]) << 16)
                | (std::uint32_t(block[4*i+2]) << 8) | block[4*i+3];
        for (unsigned i = 16; i < 80; ++i) w[i] = rotate(w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16], 1);
        auto a = state_[0], b = state_[1], c = state_[2], d = state_[3], e = state_[4];
        for (unsigned i = 0; i < 80; ++i) {
            std::uint32_t f, k;
            if (i < 20) { f = (b & c) | (~b & d); k = 0x5a827999; }
            else if (i < 40) { f = b ^ c ^ d; k = 0x6ed9eba1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8f1bbcdc; }
            else { f = b ^ c ^ d; k = 0xca62c1d6; }
            const auto next = rotate(a, 5) + f + e + k + w[i];
            e = d; d = c; c = rotate(b, 30); b = a; a = next;
        }
        state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d; state_[4] += e;
    }
    void Sha1::update(const void* data, std::size_t size) {
        if (finalized_) throw std::logic_error("SHA1 already finalized");
        if (size && !data) throw std::invalid_argument("Null SHA1 input");
        if (size > std::numeric_limits<std::uint64_t>::max() / 8 - bytes_)
            throw std::length_error("SHA1 input too large");
        bytes_ += size;
        const auto* input = static_cast<const std::uint8_t*>(data);
        while (size) {
            const auto count = std::min(size, buffer_.size() - used_);
            std::memcpy(buffer_.data() + used_, input, count);
            used_ += count; input += count; size -= count;
            if (used_ == buffer_.size()) { transform(buffer_.data()); used_ = 0; }
        }
    }
    Sha1Digest Sha1::finalize() {
        if (finalized_) throw std::logic_error("SHA1 already finalized");
        const auto bits = bytes_ * 8;
        buffer_[used_++] = 0x80;
        if (used_ > 56) {
            std::fill(buffer_.begin() + used_, buffer_.end(), 0);
            transform(buffer_.data()); used_ = 0;
        }
        std::fill(buffer_.begin() + used_, buffer_.begin() + 56, 0);
        for (unsigned i = 0; i < 8; ++i) buffer_[63-i] = static_cast<std::uint8_t>(bits >> (8*i));
        transform(buffer_.data()); finalized_ = true;
        Sha1Digest digest{};
        for (unsigned i = 0; i < 20; ++i) digest[i] = static_cast<std::uint8_t>(state_[i/4] >> (24 - 8*(i%4)));
        return digest;
    }
    std::string to_hex(const Sha1Digest& digest) {
        constexpr char digits[] = "0123456789abcdef";
        std::string result;
        result.reserve(40);
        for (auto byte : digest) { result += digits[byte >> 4]; result += digits[byte & 15]; }
        return result;
    }
}
