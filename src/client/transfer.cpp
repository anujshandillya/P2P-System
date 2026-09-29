#include "client/transfer.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fcntl.h>
#include <iomanip>
#include <map>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <sstream>
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
        const auto payload = Request::parse(request.fields()).fields();
        fields.insert(fields.end(), payload.begin(), payload.end());
        const auto deadline = Clock::now() + std::chrono::seconds(12);
        for (int attempt = 0; attempt < 3; ++attempt) {
            for (const auto& tracker : trackers) {
                try {
                    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
                    if (remaining <= 0) throw std::runtime_error("Tracker deadline expired");
                    auto response = Response::parse(rpc(tracker, fields, static_cast<int>(std::min<long long>(6500, remaining))));
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
            bool withdraw_pending = false;
            Request withdrawal;
        };
        struct Job {
            Key key;
            std::string session;
            std::shared_ptr<Source> source;
            std::shared_ptr<Fd> directory;
            std::string temporary;
            std::vector<unsigned char> states; // Missing, in flight, verified.
            std::vector<unsigned> attempts;
            std::vector<Clock::time_point> retry_at;
            std::vector<std::string> peers;
            std::size_t in_flight = 0;
            std::size_t completed = 0;
            std::string status = "D", error;
            bool finalizing = false, announced = false;
            bool terminal_reported = false;
            Request announcement, revocation;
            bool withdrawn = false;
            Clock::time_point last_progress = Clock::now();
            dev_t device{};
            ino_t inode{};
            ~Job() { if (directory && !temporary.empty()) ::unlinkat(directory->get(), temporary.c_str(), 0); }
        };
        std::vector<std::shared_ptr<Job>> jobs;
        std::vector<std::thread> downloaders;
        std::size_t job_cursor = 0;
        std::array<Endpoint, 2> trackers;
        std::string endpoint_text;
        Fd listener;
        mutable std::mutex mutex;
        std::mutex publication_mutex; // Orders background announcements before explicit revocations.
        std::condition_variable wake;
        std::atomic<bool> stopping{false};
        std::string token;
        std::map<Key, std::shared_ptr<Source>> sources;
        std::deque<Fd> connections;
        std::thread acceptor, maintenance;
        std::vector<std::thread> servers;
        struct Authorization { Clock::time_point check_after, outage_deadline; };
        // Recheck permissions every two seconds while trackers are reachable. Existing
        // authorized transfers may use a bounded two-minute grace during an outage.
        std::map<std::string, Authorization> authorizations;

        Impl(const std::string& endpoint, std::array<Endpoint, 2> endpoints)
            : trackers(std::move(endpoints)), endpoint_text(endpoint), listener(listen_on(parse_endpoint(endpoint))) {
            if (::fcntl(listener.get(), F_SETFL, O_NONBLOCK) < 0) throw std::runtime_error("Cannot configure peer listener");
            try {
                for (int i = 0; i < 4; ++i) servers.emplace_back([this] { serve(); });
                for (int i = 0; i < 4; ++i) downloaders.emplace_back([this] { download_loop(); });
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
            for (auto& downloader : downloaders) if (downloader.joinable()) downloader.join();
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
                if (cached != authorizations.end() && Clock::now() < cached->second.check_after) return;
            }
            Response response;
            try {
                response = tracker_rpc(trackers, {random_id(), request[4], "authorize", {request[1], request[2], request[3], own}});
            } catch (...) {
                std::lock_guard<std::mutex> lock(mutex);
                auto cached = authorizations.find(key);
                if (cached != authorizations.end() && Clock::now() < cached->second.outage_deadline) {
                    cached->second.check_after = std::min(Clock::now() + std::chrono::seconds(2), cached->second.outage_deadline);
                    return;
                }
                throw; // New requests require online authorization.
            }
            if (response.status != "OK") {
                std::lock_guard<std::mutex> lock(mutex);
                authorizations.erase(key);
                throw std::runtime_error("Transfer authorization denied");
            }
            std::lock_guard<std::mutex> lock(mutex);
            if (authorizations.size() >= 256) authorizations.clear();
            authorizations[key] = {Clock::now() + std::chrono::seconds(2), Clock::now() + std::chrono::seconds(120)};
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
                { std::lock_guard<std::mutex> lock(mutex); source->active = false; source->withdraw_pending = true; }
                try {
                    std::lock_guard<std::mutex> publication(publication_mutex);
                    bool same;
                    { std::lock_guard<std::mutex> lock(mutex); auto current = sources.find(key); same = token == own && current != sources.end() && current->second == source; }
                    if (same) {
                        auto result = tracker_rpc(trackers, source->withdrawal);
                        if (result.status == "OK" || result.status == "NOT_FOUND") {
                            std::lock_guard<std::mutex> lock(mutex); source->withdraw_pending = false;
                        }
                    }
                } catch (...) {}
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
        void fail(const std::shared_ptr<Job>& job, const std::string& error) {
            // Called with mutex held. Other workers finish I/O but discard their results.
            if (job->status != "D") return; // Preserve the first terminal result.
            job->status = "F"; job->error = error; job->source->active = false;
            wake.notify_all();
        }
        void finish(const std::shared_ptr<Job>& job) {
            const auto& metadata = job->source->file.metadata;
            Sha1 whole;
            for (std::size_t i = 0; i < metadata.piece_hashes.size(); ++i) {
                if (stopping) throw std::runtime_error("Download cancelled");
                auto bytes = read_piece(job->source->file.descriptor->get(), metadata, i);
                whole.update(bytes.data(), bytes.size());
            }
            if (whole.finalize() != metadata.whole_hash) throw std::runtime_error("Whole-file SHA1 mismatch");
            if (::fsync(job->source->file.descriptor->get()) < 0) throw std::runtime_error("Cannot flush downloaded file");
            // Serialize the final name installation with cancellation. linkat is an
            // atomic no-replace operation within the pinned destination directory.
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping || job->status != "D" || token != job->session) throw std::runtime_error("Download cancelled");
            if (::linkat(job->directory->get(), job->temporary.c_str(), job->directory->get(), metadata.name.c_str(), 0) < 0)
                throw std::runtime_error("Cannot finalize download (destination exists or is not writable)");
            ::unlinkat(job->directory->get(), job->temporary.c_str(), 0);
            job->temporary.clear();
            job->status = "C"; job->error.clear(); job->finalizing = false;
            wake.notify_all();
        }
        void download_loop() {
            while (!stopping) {
                std::shared_ptr<Job> job;
                std::size_t index = 0;
                std::string peer;
                bool finishing = false;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    for (std::size_t n = 0; n < jobs.size(); ++n) {
                        auto candidate = jobs[(job_cursor + n) % jobs.size()];
                        if (candidate->status != "D" || candidate->finalizing) continue;
                        if (candidate->completed == candidate->states.size() && candidate->in_flight == 0) {
                            candidate->finalizing = true; job = candidate; finishing = true;
                        } else if (!candidate->peers.empty()) {
                            for (std::size_t i = 0; i < candidate->states.size(); ++i) {
                                if (candidate->states[i] != 0 || Clock::now() < candidate->retry_at[i]) continue;
                                candidate->states[i] = 1; ++candidate->in_flight;
                                peer = candidate->peers[(i + candidate->attempts[i]) % candidate->peers.size()];
                                index = i; job = candidate; break;
                            }
                        }
                        if (job) { job_cursor = (job_cursor + n + 1) % jobs.size(); break; }
                    }
                    if (!job) { wake.wait_for(lock, std::chrono::milliseconds(100)); continue; }
                }
                if (finishing) {
                    try { finish(job); }
                    catch (const std::exception& e) { std::lock_guard<std::mutex> lock(mutex); fail(job, e.what()); }
                    continue;
                }
                try {
                    const auto& metadata = job->source->file.metadata;
                    const auto identity = to_hex(metadata.whole_hash);
                    const auto endpoint = parse_endpoint(peer);
                    const auto bits = rpc(endpoint, {"BITFIELD", job->key.first, job->key.second, identity, job->session}, 9000);
                    if (bits.size() != 3 || bits[0] != "OK" || bits[1] != identity
                        || bits[2].size() != metadata.piece_hashes.size() || bits[2][index] != '1')
                        throw std::runtime_error("Peer does not have this piece");
                    const auto response = rpc(endpoint, {"PIECE", job->key.first, job->key.second, identity, job->session, std::to_string(index)}, 9000);
                    const auto offset = std::uint64_t(index) * piece_size;
                    const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(piece_size, metadata.size - offset));
                    if (response.size() != 4 || response[0] != "OK" || response[1] != identity
                        || response[2] != std::to_string(index) || response[3].size() != length)
                        throw std::runtime_error("Invalid piece response");
                    Sha1 hash; hash.update(response[3].data(), length);
                    if (hash.finalize() != metadata.piece_hashes[index]) throw std::runtime_error("Peer returned a corrupt piece");
                    std::size_t done = 0;
                    while (done < length) {
                        const auto count = ::pwrite(job->source->file.descriptor->get(), response[3].data() + done,
                                                length - done, static_cast<off_t>(offset + done));
                        if (count < 0 && errno == EINTR) continue;
                        if (count <= 0) {
                            std::lock_guard<std::mutex> lock(mutex);
                            fail(job, "Cannot write downloaded piece (check disk space and permissions)");
                            throw std::runtime_error("File write failed");
                        }
                        done += static_cast<std::size_t>(count);
                    }
                    std::lock_guard<std::mutex> lock(mutex);
                    --job->in_flight;
                    if (job->status == "D" && token == job->session) {
                        job->states[index] = 2; job->source->verified[index] = 1; ++job->completed;
                        job->last_progress = Clock::now(); job->error.clear();
                    }
                    wake.notify_all();
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(mutex);
                    --job->in_flight;
                    job->states[index] = 0;
                    const auto attempts = ++job->attempts[index];
                    job->retry_at[index] = Clock::now() + std::chrono::milliseconds(std::min(2000U, 100U * attempts));
                    if (job->status == "D") job->error = e.what();
                }
            }
        }
        void maintain() {
            while (!stopping) {
                std::string own;
                std::vector<std::shared_ptr<Job>> current;
                std::vector<std::pair<Key, std::shared_ptr<Source>>> withdrawals;
                {
                    std::lock_guard<std::mutex> lock(mutex); own = token; current = jobs;
                    for (const auto& source : sources) if (source.second->withdraw_pending) withdrawals.push_back(source);
                }
                if (!own.empty()) {
                    for (const auto& tracker : trackers) {
                        try { (void)rpc(tracker, {"CLIENT", random_id(), own, "heartbeat"}, 1000); } catch (...) {}
                    }
                    for (const auto& item : withdrawals) {
                        if (stopping) break;
                        try {
                            std::lock_guard<std::mutex> publication(publication_mutex);
                            {
                                std::lock_guard<std::mutex> lock(mutex);
                                auto source = sources.find(item.first);
                                if (token != own || source == sources.end() || source->second != item.second || !item.second->withdraw_pending) continue;
                            }
                            const auto result = tracker_rpc(trackers, item.second->withdrawal);
                            if (result.status == "OK" || result.status == "NOT_FOUND" || result.status == "FORBIDDEN") {
                                std::lock_guard<std::mutex> lock(mutex); item.second->withdraw_pending = false;
                            }
                        } catch (...) {}
                    }
                    for (const auto& job : current) {
                        if (stopping) break;
                        bool announce, refresh, withdraw;
                        {
                            std::lock_guard<std::mutex> lock(mutex);
                            if (job->session != own) continue;
                            withdraw = job->status == "F" && !job->withdrawn;
                            auto source = sources.find(job->key);
                            if (source != sources.end() && source->second != job->source) withdraw = false;
                            if (job->status == "D" && Clock::now() - job->last_progress > std::chrono::seconds(120)) {
                                fail(job, "No download progress for 120 seconds; retry download_file"); continue;
                            }
                            announce = !job->announced && job->source->active && (job->completed > 0 || job->status == "C");
                            refresh = job->status == "D";
                        }
                        if (withdraw) {
                            try {
                                std::lock_guard<std::mutex> publication(publication_mutex);
                                {
                                    std::lock_guard<std::mutex> lock(mutex);
                                    auto current_source = sources.find(job->key);
                                    if (job->withdrawn || token != own || (current_source != sources.end() && current_source->second != job->source)) continue;
                                }
                                auto result = tracker_rpc(trackers, job->revocation);
                                std::lock_guard<std::mutex> lock(mutex);
                                job->withdrawn = result.status == "OK" || result.status == "NOT_FOUND" || result.status == "FORBIDDEN";
                            } catch (...) {}
                        }
                        if (announce) {
                            try {
                                std::lock_guard<std::mutex> publication(publication_mutex);
                                {
                                    std::lock_guard<std::mutex> lock(mutex);
                                    if (token != own || !job->source->active || job->status == "F") continue;
                                }
                                const auto response = tracker_rpc(trackers, job->announcement);
                                std::lock_guard<std::mutex> lock(mutex);
                                if (response.status == "OK") job->announced = true;
                                else if (response.status != "RETRY_LATER") {
                                    if (job->status == "D") fail(job, response.message);
                                    else job->source->active = false;
                                }
                            } catch (...) {} // Same request ID is retained for uncertain publication.
                        }
                        if (refresh) {
                            try {
                                const auto response = tracker_rpc(trackers, {random_id(), own, "discover", {job->key.first, job->key.second}});
                                std::lock_guard<std::mutex> lock(mutex);
                                if (job->status != "D") continue;
                                if (response.status == "OK" && !response.items.empty()
                                    && serialize_metadata(parse_metadata(response.items[0])) == serialize_metadata(job->source->file.metadata)) {
                                    job->peers.assign(response.items.begin() + 1, response.items.end());
                                    // The local partial share cannot help this client's missing pieces.
                                    job->peers.erase(std::remove(job->peers.begin(), job->peers.end(), endpoint_text), job->peers.end());
                                    wake.notify_all();
                                } else if (response.status == "FORBIDDEN" || response.status == "UNAUTHENTICATED" || response.status == "NOT_FOUND")
                                    fail(job, response.message);
                            } catch (...) {} // Existing peers remain usable during tracker outage.
                        }
                    }
                }
                std::unique_lock<std::mutex> lock(mutex);
                wake.wait_for(lock, std::chrono::seconds(2), [this, &own] { return stopping || token != own; });
            }
        }

    };
    TransferManager::TransferManager(const std::string& endpoint, std::array<Endpoint, 2> trackers)
        : impl_(std::make_unique<Impl>(endpoint, std::move(trackers))) {}
    TransferManager::~TransferManager() = default;
    PreparedFile TransferManager::prepare(const std::string& group, const std::string& path) {
        if (path.empty() || path.find('\0') != std::string::npos) throw std::runtime_error("Invalid file path");
        auto fd = std::make_shared<Fd>(::open(path.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC));
        if (fd->get() < 0) throw std::runtime_error("Cannot open source file");
        const auto slash = path.find_last_of('/');
        auto metadata = inspect_file(fd->get(), path.substr(slash == std::string::npos ? 0 : slash + 1));
        (void)parse_metadata(serialize_metadata(metadata));
        std::lock_guard<std::mutex> publication(impl_->publication_mutex);
        std::lock_guard<std::mutex> lock(impl_->mutex);
        const Key key{group, metadata.name};
        for (const auto& job : impl_->jobs)
            if (job->key == key && job->status == "D") throw std::runtime_error("A download for this file is already active");
        for (auto& job : impl_->jobs) if (job->key == key) job->withdrawn = true;
        if (impl_->sources.size() >= 256 && !impl_->sources.count(key)) throw std::runtime_error("Local share limit reached (256)");
        return {std::move(metadata), std::move(fd)};
    }
    void TransferManager::set_session(const std::string& token) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        for (auto& source : impl_->sources) source.second->active = false;
        for (auto& job : impl_->jobs) if (job->status == "D") impl_->fail(job, "Session ended");
        impl_->sources.clear(); impl_->authorizations.clear(); impl_->token = token;
        impl_->wake.notify_all();
    }
    Response TransferManager::publish(const Request& request, const PreparedFile& file) {
        std::lock_guard<std::mutex> publication(impl_->publication_mutex);
        auto response = tracker_rpc(impl_->trackers, request);
        if (response.status == "OK") share(request.args.at(0), file);
        return response;
    }
    void TransferManager::share(const std::string& group, const PreparedFile& file) {
        auto source = std::make_shared<Impl::Source>();
        source->file = file; source->verified.assign(file.metadata.piece_hashes.size(), 1);
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->token.empty()) throw std::runtime_error("Session revoked before sharing");
        const Key key{group, file.metadata.name};
        source->withdrawal = {random_id(), impl_->token, "stop_share", {group, file.metadata.name}};
        for (const auto& job : impl_->jobs)
            if (job->key == key && job->status == "D") throw std::runtime_error("A download for this file is already active");
        auto previous = impl_->sources.find(key);
        if (previous != impl_->sources.end()) previous->second->active = false;
        impl_->sources[key] = std::move(source);
    }
    void TransferManager::stop_share(const std::string& group, const std::string& name) {
        std::lock_guard<std::mutex> publication(impl_->publication_mutex);
        std::lock_guard<std::mutex> lock(impl_->mutex);
        for (auto& job : impl_->jobs) if (job->key == Key{group, name}) job->source->active = false;
        auto source = impl_->sources.find({group, name});
        if (source != impl_->sources.end()) { source->second->active = false; impl_->sources.erase(source); }
    }
    void TransferManager::leave_group(const std::string& group) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        for (auto& job : impl_->jobs) if (job->key.first == group && job->status == "D") impl_->fail(job, "Left group");
        for (auto it = impl_->sources.begin(); it != impl_->sources.end(); ) {
            if (it->first.first == group) { it->second->active = false; it = impl_->sources.erase(it); } else ++it;
        }
    }

    void TransferManager::download(const std::string& group, const std::string& name, const std::string& directory) {
        std::string token;
        { std::lock_guard<std::mutex> lock(impl_->mutex); token = impl_->token; }
        if (token.empty()) throw std::runtime_error("Login before downloading");
        const auto response = tracker_rpc(impl_->trackers, {random_id(), token, "discover", {group, name}});
        if (response.status != "OK" || response.items.empty()) throw std::runtime_error(response.message);
        auto metadata = parse_metadata(response.items[0]);
        if (metadata.name != name) throw std::runtime_error("Unexpected file metadata");
        if (directory.empty() || directory.find('\0') != std::string::npos) throw std::runtime_error("Invalid destination directory");
        auto destination = std::make_shared<Fd>(::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        struct stat info{}, existing{};
        if (destination->get() < 0 || ::fstat(destination->get(), &info) < 0) throw std::runtime_error("Destination must be an accessible directory");
        if (::fstatat(destination->get(), name.c_str(), &existing, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
            throw std::runtime_error("Destination already exists or cannot be inspected");
        auto job = std::make_shared<Impl::Job>();
        job->key = {group, name}; job->session = token; job->directory = destination;
        job->device = info.st_dev; job->inode = info.st_ino;
        job->temporary = ".p2p-" + random_id() + ".part";
        auto fd = std::make_shared<Fd>(::openat(destination->get(), job->temporary.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
        if (fd->get() < 0 || ::ftruncate(fd->get(), static_cast<off_t>(metadata.size)) < 0)
            throw std::runtime_error("Cannot create temporary download file");
        job->source = std::make_shared<Impl::Source>();
        job->source->file = {metadata, std::move(fd)};
        job->source->verified.assign(metadata.piece_hashes.size(), 0);
        job->states.assign(metadata.piece_hashes.size(), 0);
        job->attempts.assign(metadata.piece_hashes.size(), 0);
        job->retry_at.resize(metadata.piece_hashes.size());
        job->peers.assign(response.items.begin() + 1, response.items.end());
        job->peers.erase(std::remove(job->peers.begin(), job->peers.end(), impl_->endpoint_text), job->peers.end());
        job->announcement = {random_id(), token, "upload_file", {group, serialize_metadata(metadata)}};
        job->revocation = {random_id(), token, "stop_share", {group, name}};
        job->source->withdrawal = job->revocation;
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (token != impl_->token) throw std::runtime_error("Session changed");
        if (impl_->jobs.size() >= 64) throw std::runtime_error("Download history limit reached (64 jobs); restart client");
        std::size_t active = 0;
        for (const auto& current : impl_->jobs) if (current->status == "D") {
            ++active;
            if (current->key == job->key || (current->device == job->device && current->inode == job->inode && current->key.second == name))
                throw std::runtime_error("This file or destination already has an active download");
        }
        if (active >= 16) throw std::runtime_error("Too many active downloads (maximum 16)");
        auto previous = impl_->sources.find(job->key);
        if (previous != impl_->sources.end() && previous->second->active) throw std::runtime_error("Already sharing this file locally");
        impl_->sources[job->key] = job->source;
        impl_->jobs.push_back(job); impl_->wake.notify_all();
    }
    Fields TransferManager::downloads() const {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        Fields out;
        for (const auto& job : impl_->jobs) {
            std::string line = "[" + job->status + "] [" + job->key.first + "] " + job->key.second;
            if (job->status == "D") line += " " + std::to_string(job->completed) + "/" + std::to_string(job->states.size()) + " pieces";
            if (!job->error.empty()) line += " (" + job->error + ")";
            out.push_back(std::move(line));
        }
        return out;
    }
    Fields TransferManager::take_notifications() {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        Fields out;
        for (const auto& job : impl_->jobs) {
            if (job->status == "D" || job->terminal_reported) continue;
            const auto& metadata = job->source->file.metadata;
            std::ostringstream message;
            message << "[DOWNLOAD " << (job->status == "C" ? "COMPLETED" : "FAILED") << "]"
                    << " group=" << std::quoted(job->key.first)
                    << " file=" << std::quoted(job->key.second)
                    << " bytes=" << metadata.size
                    << " pieces=" << job->completed << '/' << job->states.size();
            if (job->status == "C")
                message << " integrity=VERIFIED sha1=" << to_hex(metadata.whole_hash);
            else
                message << " reason=" << std::quoted(job->error);
            out.push_back(message.str());
            job->terminal_reported = true;
        }
        return out;
    }
}
