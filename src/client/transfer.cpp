#include "client/transfer.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace p2p {
namespace {
using Clock = std::chrono::steady_clock;
using Key = std::pair<std::string, std::string>;
Response tracker_rpc(const std::array<Endpoint, 2>& trackers, const Request& request) {
    Fields fields{"CLIENT"};
    const auto payload = request.fields();
    fields.insert(fields.end(), payload.begin(), payload.end());
    for (int attempt = 0; attempt < 3; ++attempt) {
        for (const auto& tracker : trackers) {
            try {
                auto response = Response::parse(rpc(tracker, fields, 1800));
                if (response.status != "RETRY_LATER") return response;
            } catch (const std::exception&) {}
        }
    }
    throw std::runtime_error("Trackers unavailable; try again");
}
std::string read_piece(int fd, const FileMetadata& metadata, std::size_t index) {
    if (index >= metadata.piece_hashes.size()) throw std::runtime_error("Invalid piece index");
    const auto offset = std::uint64_t(index) * piece_size;
    std::string bytes(static_cast<std::size_t>(std::min<std::uint64_t>(piece_size, metadata.size - offset)), '\0');
    std::size_t done = 0;
    while (done < bytes.size()) {
        const auto n = ::pread(fd, bytes.data() + done, bytes.size() - done, static_cast<off_t>(offset + done));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("Source file is missing or changed");
        done += static_cast<std::size_t>(n);
    }
    Sha1 hash; hash.update(bytes.data(), bytes.size());
    if (hash.finalize() != metadata.piece_hashes[index]) throw std::runtime_error("Source piece hash mismatch");
    return bytes;
}
}
struct TransferManager::Impl {
    struct Source {
        PreparedFile file;
        std::vector<unsigned char> verified;
        bool active = true;
    };
    std::array<Endpoint, 2> trackers;
    Fd listener;
    mutable std::mutex mutex;
    std::condition_variable wake;
    std::atomic<bool> stopping{false};
    std::string token;
    std::map<Key, std::shared_ptr<Source>> sources;
    std::deque<Fd> connections;
    std::thread acceptor, maintenance;
    std::vector<std::thread> servers;
    // Authorization lasts at most two seconds; local revocation is checked per request.
    std::map<std::string, Clock::time_point> authorizations;

    Impl(const std::string& endpoint, std::array<Endpoint, 2> endpoints)
        : trackers(std::move(endpoints)), listener(listen_on(parse_endpoint(endpoint))) {
        if (::fcntl(listener.get(), F_SETFL, O_NONBLOCK) < 0) throw std::runtime_error("Cannot configure peer listener");
        try {
            for (int i = 0; i < 4; ++i) servers.emplace_back([this] { serve(); });
            acceptor = std::thread([this] { accept_loop(); });
            maintenance = std::thread([this] { maintain(); });
        } catch (...) { stop(); throw; }
    }
    ~Impl() { stop(); }
    void stop() {
        stopping = true; wake.notify_all();
        if (acceptor.joinable()) acceptor.join();
        if (maintenance.joinable()) maintenance.join();
        for (auto& server : servers) if (server.joinable()) server.join();
    }
    void accept_loop() {
        while (!stopping) {
            pollfd p{listener.get(), POLLIN, 0};
            if (::poll(&p, 1, 100) <= 0 || !(p.revents & POLLIN)) continue;
            Fd fd(::accept(listener.get(), nullptr, nullptr));
            if (fd.get() < 0) continue;
            std::lock_guard<std::mutex> lock(mutex);
            if (connections.size() < 64) { connections.push_back(std::move(fd)); wake.notify_all(); }
        }
    }
    void authorize(const Fields& request, const std::string& own) {
        const auto key = encode({request[1], request[2], request[3], request[4], own});
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto cached = authorizations.find(key);
            if (cached != authorizations.end() && Clock::now() < cached->second) return;
        }
        auto response = tracker_rpc(trackers, {random_id(), request[4], "authorize", {request[1], request[2], request[3], own}});
        if (response.status != "OK") throw std::runtime_error("Transfer authorization denied");
        std::lock_guard<std::mutex> lock(mutex);
        if (authorizations.size() >= 256) authorizations.clear();
        authorizations[key] = Clock::now() + std::chrono::seconds(2);
    }
    Fields handle(const Fields& request) {
        if (request.size() < 5 || request.size() > 6 || (request[0] != "BITFIELD" && request[0] != "PIECE")
            || request.size() != (request[0] == "PIECE" ? 6U : 5U)) throw std::runtime_error("Invalid peer request");
        for (const auto& field : request) if (field.size() > 256) throw std::runtime_error("Peer field too long");
        const Key key{request[1], request[2]};
        std::shared_ptr<Source> source;
        std::string own;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto found = sources.find(key);
            if (token.empty() || found == sources.end() || !found->second->active) throw std::runtime_error("Share unavailable");
            source = found->second; own = token;
        }
        if (request[3] != to_hex(source->file.metadata.whole_hash)) throw std::runtime_error("Wrong file identity");
        authorize(request, own);
        std::size_t index = request[0] == "PIECE" ? static_cast<std::size_t>(number(request[5])) : 0;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (token != own || !source->active) throw std::runtime_error("Share revoked");
            if (request[0] == "BITFIELD") {
                std::string bits;
                for (auto bit : source->verified) bits += bit ? '1' : '0';
                return {"OK", request[3], bits};
            }
            if (index >= source->verified.size() || !source->verified[index]) throw std::runtime_error("Piece unavailable");
        }
        std::string bytes;
        try { bytes = read_piece(source->file.descriptor->get(), source->file.metadata, index); }
        catch (...) {
            { std::lock_guard<std::mutex> lock(mutex); source->active = false; }
            try { (void)tracker_rpc(trackers, {random_id(), own, "stop_share", {key.first, key.second}}); } catch (...) {}
            throw;
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (token != own || !source->active) throw std::runtime_error("Share revoked");
        }
        return {"OK", request[3], request[5], std::move(bytes)};
    }
    void serve() {
        while (!stopping) {
            Fd connection;
            {
                std::unique_lock<std::mutex> lock(mutex);
                wake.wait(lock, [this] { return stopping || !connections.empty(); });
                if (stopping) return;
                connection = std::move(connections.front()); connections.pop_front();
            }
            try { send_frame(connection.get(), handle(receive_frame(connection.get(), 1500)), 2000); }
            catch (const std::exception& e) {
                try { send_frame(connection.get(), {"ERROR", e.what()}, 200); } catch (...) {}
            }
        }
    }
    void maintain() {
        while (!stopping) {
            std::string own;
            { std::lock_guard<std::mutex> lock(mutex); own = token; }
            if (!own.empty()) {
                for (const auto& tracker : trackers) {
                    try { (void)rpc(tracker, {"CLIENT", random_id(), own, "heartbeat"}, 1000); } catch (...) {}
                }
            }
            std::unique_lock<std::mutex> lock(mutex);
            wake.wait_for(lock, std::chrono::seconds(3), [this, &own] { return stopping || token != own; });
        }
    }
};
TransferManager::TransferManager(const std::string& endpoint, std::array<Endpoint, 2> trackers)
    : impl_(std::make_unique<Impl>(endpoint, std::move(trackers))) {}
TransferManager::~TransferManager() = default;
PreparedFile TransferManager::prepare(const std::string& path) {
    auto metadata = inspect_file(path);
    (void)parse_metadata(serialize_metadata(metadata));
    auto fd = std::make_shared<Fd>(::open(path.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC));
    struct stat info{};
    if (fd->get() < 0 || ::fstat(fd->get(), &info) < 0 || !S_ISREG(info.st_mode)
        || info.st_size < 0 || static_cast<std::uint64_t>(info.st_size) != metadata.size)
        throw std::runtime_error("Source changed after inspection");
    return {std::move(metadata), std::move(fd)};
}
void TransferManager::set_session(const std::string& token) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    for (auto& source : impl_->sources) source.second->active = false;
    impl_->sources.clear(); impl_->authorizations.clear(); impl_->token = token;
    impl_->wake.notify_all();
}
void TransferManager::share(const std::string& group, const PreparedFile& file) {
    auto source = std::make_shared<Impl::Source>();
    source->file = file; source->verified.assign(file.metadata.piece_hashes.size(), 1);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->token.empty()) throw std::runtime_error("Session revoked before sharing");
    const Key key{group, file.metadata.name};
    auto previous = impl_->sources.find(key);
    if (previous != impl_->sources.end()) previous->second->active = false;
    impl_->sources[key] = std::move(source);
}
void TransferManager::stop_share(const std::string& group, const std::string& name) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto source = impl_->sources.find({group, name});
    if (source != impl_->sources.end()) { source->second->active = false; impl_->sources.erase(source); }
}
void TransferManager::leave_group(const std::string& group) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    for (auto it = impl_->sources.begin(); it != impl_->sources.end(); ) {
        if (it->first.first == group) { it->second->active = false; it = impl_->sources.erase(it); } else ++it;
    }
}
}
