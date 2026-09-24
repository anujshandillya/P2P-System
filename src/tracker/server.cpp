#include "tracker/server.hpp"
#include <cerrno>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace p2p {
    namespace {
        volatile std::sig_atomic_t interrupted = 0;
        void signal_handler(int) { interrupted = 1; }
    }

    TrackerServer::TrackerServer(int id, std::array<Endpoint, 2> endpoints, const std::string& journal, std::string cluster_key)
        : id_(id), endpoints_(std::move(endpoints)), state_(id, journal), key_(std::move(cluster_key)),
        listener_(listen_on(endpoints_[id - 1])) {
        if (::fcntl(listener_.get(), F_SETFL, O_NONBLOCK) < 0) throw std::runtime_error("Cannot configure listener");
    }

    bool TrackerServer::synchronize() {
        try {
            Fields message{"SYNC", key_};
            auto events = state_.events();
            message.insert(message.end(), events.begin(), events.end());
            const auto response = rpc(endpoints_[2 - id_], message, 1200);
            if (response.empty() || response[0] != "SYNC_OK") throw std::runtime_error("Replication rejected");
            state_.merge(Fields(response.begin() + 1, response.end()));
            connected_ = true;
            return true;
        } catch (const std::exception&) {
            connected_ = false;
            return false;
        }
    }

    Fields TrackerServer::handle(const Fields& message) {
        if (message.empty()) throw std::runtime_error("Empty request");
        if (message[0] == "PING") return {"PONG", std::to_string(id_)};
        if (message[0] == "SYNC") {
            if (message.size() < 2 || message[1] != key_)
                return Response{"FORBIDDEN", "Invalid tracker credentials", "local", "", {}}.fields();
            // Replication must never wait for the foreground operation lock: the
            // foreground may be waiting for its partner's replication response.
            state_.merge(Fields(message.begin() + 2, message.end()));
            Fields response{"SYNC_OK"};
            const auto events = state_.events();
            response.insert(response.end(), events.begin(), events.end());
            return response;
        }
        bool forwarded = message[0] == "FORWARD";
        if (message[0] != "CLIENT" && !forwarded) throw std::runtime_error("Unknown message type");
        const std::size_t offset = forwarded ? 2 : 1;
        if (forwarded && (message.size() < 2 || message[1] != key_))
            return Response{"FORBIDDEN", "Invalid tracker credentials", "local", "", {}}.fields();
        if (forwarded && id_ != 1) throw std::runtime_error("Only tracker 1 accepts forwarded requests");
        const auto req = Request::parse(Fields(message.begin() + static_cast<std::ptrdiff_t>(offset), message.end()));

        // Reject briefly rather than occupy every worker waiting on one command.
        // This leaves worker capacity for the peer's synchronization requests.
        std::unique_lock<std::mutex> operation(operation_mutex_, std::try_to_lock);
        if (!operation.owns_lock())
            return Response{"RETRY_LATER", "Tracker is applying another operation; retry with the same request ID", "local", "", {}}.fields();

        if (id_ == 2 && !forwarded) {
            try {
                Fields forwarded_message{"FORWARD", key_};
                const auto fields = req.fields();
                forwarded_message.insert(forwarded_message.end(), fields.begin(), fields.end());
                return Response::parse(rpc(endpoints_[0], forwarded_message, 4500)).fields();
            } catch (const std::exception&) {
                // An uncertain forward is safe to retry locally with the original
                // request ID; reconnect replay deduplicates accepted copies.
            }
        }
        synchronize(); // Recover remote operations before answering from local state.
        auto response = state_.execute(req);
        const bool replicated = synchronize();
        // Reconciliation may have changed the outcome of an accepted local event.
        if (response.status == "OK") response = state_.execute(req);
        response.mode = replicated ? "replicated" : "degraded";
        return response.fields();
    }

    void TrackerServer::worker() {
        for (;;) {
            Fd connection;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                wake_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
                if (stopping_) return;
                connection = std::move(queue_.front());
                queue_.pop_front();
            }
            try {
                const auto request = receive_frame(connection.get(), 2000);
                const auto response = handle(request);
                send_frame(connection.get(), response, 3000);
            } catch (const std::exception& e) {
                // Error text contains protocol/storage diagnostics, never request payloads.
                try { send_frame(connection.get(), Response{"ERROR", e.what(), "local", "", {}}.fields(), 300); }
                catch (const std::exception&) {}
            }
        }
    }

    void TrackerServer::replication_loop() {
        while (!stopping_) {
            {
                std::unique_lock<std::mutex> operation(operation_mutex_, std::try_to_lock);
                if (operation.owns_lock()) synchronize();
            }
            std::unique_lock<std::mutex> lock(queue_mutex_);
            wake_.wait_for(lock, std::chrono::milliseconds(300), [this] { return stopping_.load(); });
        }
    }

    void TrackerServer::stop() {
        stopping_ = true;
        wake_.notify_all();
        if (replicator_.joinable()) replicator_.join();
        for (auto& worker_thread : workers_) if (worker_thread.joinable()) worker_thread.join();
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.clear();
    }

    void TrackerServer::run() {
        interrupted = 0;
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        // The listener is already bound, but client requests are accepted only after
        // one initial recovery attempt (or a bounded transition to degraded mode).
        synchronize();
        try {
            for (int i = 0; i < 8; ++i) workers_.emplace_back(&TrackerServer::worker, this);
            replicator_ = std::thread(&TrackerServer::replication_loop, this);
            std::cout << "Tracker " << id_ << " ready at " << endpoints_[id_ - 1].ip << ':' << endpoints_[id_ - 1].port
                    << " (" << (connected_ ? "replicated" : "degraded") << ") " << state_.summary() << std::endl;
            std::string console;
            bool console_open = true;
            while (!interrupted && !stopping_) {
                pollfd descriptors[2]{{listener_.get(), POLLIN, 0}, {console_open ? STDIN_FILENO : -1, POLLIN, 0}};
                const int ready = ::poll(descriptors, 2, 100);
                if (ready < 0 && errno == EINTR) continue;
                if (ready < 0) throw std::runtime_error("Tracker poll failed");
                if (descriptors[0].revents & POLLIN) {
                    Fd connection(::accept(listener_.get(), nullptr, nullptr));
                    if (connection.get() >= 0) {
                        std::lock_guard<std::mutex> lock(queue_mutex_);
                        if (queue_.size() < 64) { queue_.push_back(std::move(connection)); wake_.notify_all(); }
                    } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                        throw std::runtime_error("Tracker accept failed");
                    }
                }
                if (console_open && (descriptors[1].revents & (POLLIN | POLLHUP))) {
                    char bytes[256];
                    const auto count = ::read(STDIN_FILENO, bytes, sizeof(bytes));
                    if (count == 0) console_open = false;
                    else if (count > 0) {
                        console.append(bytes, static_cast<std::size_t>(count));
                        std::size_t end;
                        while ((end = console.find('\n')) != std::string::npos) {
                            auto command = console.substr(0, end);
                            console.erase(0, end + 1);
                            if (!command.empty() && command.back() == '\r') command.pop_back();
                            if (command == "quit") stopping_ = true;
                            else if (command == "status") std::cout << (connected_ ? "replicated " : "degraded ") << state_.summary() << std::endl;
                            else if (!command.empty()) std::cout << "Console commands: status, quit" << std::endl;
                        }
                        if (console.size() > 4096) console.clear();
                    }
                }
            }
        } catch (...) { stop(); throw; }
        stop();
        std::cout << "Tracker " << id_ << " stopped" << std::endl;
    }
} // namespace p2p
