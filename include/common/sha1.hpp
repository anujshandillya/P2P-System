#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
namespace p2p {
using Sha1Digest = std::array<std::uint8_t, 20>;
class Sha1 {
public:
    Sha1() = default;
    void update(const void* data, std::size_t size);
    // Call once; subsequent updates/finalization throw.
    Sha1Digest finalize();
private:
    void transform(const std::uint8_t* block);
    std::array<std::uint32_t, 5> state_{{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0}};
    std::array<std::uint8_t, 64> buffer_{};
    std::uint64_t bytes_ = 0;
    std::size_t used_ = 0;
    bool finalized_ = false;
};
std::string to_hex(const Sha1Digest& digest);
}
