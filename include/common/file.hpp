#pragma once
#include "common/sha1.hpp"
#include <vector>
namespace p2p {
    constexpr std::uint32_t piece_size = 512 * 1024;
    constexpr std::uint64_t max_file_size = 1024ULL * 1024 * 1024;
    struct FileMetadata {
        std::string name;
        std::uint64_t size = 0;
        Sha1Digest whole_hash{};
        std::vector<Sha1Digest> piece_hashes;
    };
    FileMetadata inspect_file(const std::string& path);
    FileMetadata inspect_file(int fd, const std::string& name);
    }

    namespace p2p {
    // Validated, portable metadata encoding; local paths are never serialized.
    std::string serialize_metadata(const FileMetadata& metadata);
    FileMetadata parse_metadata(const std::string& bytes);
}
