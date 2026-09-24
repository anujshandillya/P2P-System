#pragma once
#include "common/file.hpp"
#include "common/net.hpp"
#include <memory>

namespace p2p {
struct PreparedFile {
    FileMetadata metadata;
    std::shared_ptr<Fd> descriptor;
};
class TransferManager {
public:
    TransferManager(const std::string& endpoint, std::array<Endpoint, 2> trackers);
    ~TransferManager();
    TransferManager(const TransferManager&) = delete;
    TransferManager& operator=(const TransferManager&) = delete;
    PreparedFile prepare(const std::string& group, const std::string& path);
    void set_session(const std::string& token);
    void share(const std::string& group, const PreparedFile& file);
    void stop_share(const std::string& group, const std::string& name);
    void leave_group(const std::string& group);
    void download(const std::string& group, const std::string& name, const std::string& directory);
    Fields downloads() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}
