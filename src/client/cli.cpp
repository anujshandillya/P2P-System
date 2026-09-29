#include "client/cli.hpp"
#include "common/file.hpp"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <iostream>
#include <map>
#include <poll.h>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace p2p {
    namespace {
        volatile std::sig_atomic_t exit_requested = 0;
        void request_exit(int) { exit_requested = 1; }

        // Signal handlers only set a flag. Cleanup and network I/O run on the CLI thread.
        struct ExitSignals {
            const int signals[3]{SIGINT, SIGTERM, SIGHUP};
            struct sigaction previous[3]{};
            std::size_t installed = 0;
            ExitSignals() {
                exit_requested = 0;
                struct sigaction action{};
                action.sa_handler = request_exit;
                sigemptyset(&action.sa_mask);
                for (int signal : signals) {
                    if (::sigaction(signal, &action, &previous[installed]) < 0) {
                        restore();
                        throw std::runtime_error("Cannot install client exit handlers");
                    }
                    ++installed;
                }
            }
            void restore() {
                while (installed) {
                    --installed;
                    ::sigaction(signals[installed], &previous[installed], nullptr);
                }
            }
            ~ExitSignals() { restore(); }
        };

        class CommandInput {
            char buffer_[1024]{};
            std::size_t position_ = 0, length_ = 0;
        public:
            bool next(std::string& line) {
                line.clear();
                while (!exit_requested) {
                    if (position_ != length_) {
                        const char c = buffer_[position_++];
                        if (c == '\n') return true;
                        // Retain one excess byte to report an oversized command,
                        // then discard the rest of that line without unbounded memory.
                        if (line.size() <= 4096) line += c;
                        continue;
                    }
                    pollfd input{STDIN_FILENO, POLLIN, 0};
                    const int ready = ::poll(&input, 1, 100);
                    if (ready < 0 && errno == EINTR) continue;
                    if (ready < 0 || (input.revents & POLLNVAL)) {
                        std::cerr << "Cannot read client input; exiting.\n";
                        return false;
                    }
                    if (!ready || exit_requested) continue;
                    const auto count = ::read(STDIN_FILENO, buffer_, sizeof(buffer_));
                    if (count < 0 && (errno == EINTR || errno == EAGAIN)) continue;
                    if (count < 0) {
                        std::cerr << "Cannot read client input; exiting.\n";
                        return false;
                    }
                    if (!count) return !line.empty();
                    position_ = 0;
                    length_ = static_cast<std::size_t>(count);
                }
                return false;
            }
        };
    }

    Fields split_command(const std::string& line) {
        Fields out;
        std::string word;
        char quote = 0;
        bool escape = false, started = false;
        for (char c : line) {
            if (escape) { word += c; escape = false; started = true; }
            else if (c == '\\') { escape = true; started = true; }
            else if (quote) { if (c == quote) quote = 0; else word += c; }
            else if (c == '\'' || c == '"') { quote = c; started = true; }
            else if (c == ' ' || c == '\t' || c == '\r') {
                if (started) { out.push_back(word); word.clear(); started = false; }
            } else { word += c; started = true; }
        }
        if (quote || escape) throw std::runtime_error("Unclosed quote or escape");
        if (started) out.push_back(word);
        return out;
    }

    Client::Client(std::string endpoint, std::array<Endpoint, 2> trackers, int preferred)
        : endpoint_(std::move(endpoint)), trackers_(std::move(trackers)), preferred_(preferred), transfers_(endpoint_, trackers_) {}

    Response Client::dispatch(const Request& request) {
        Fields message{"CLIENT"};
        const auto fields = request.fields();
        message.insert(message.end(), fields.begin(), fields.end());
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
        for (int attempt = 0; attempt < 8; ++attempt) {
            for (int offset = 0; offset < 2; ++offset) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
                if (remaining <= 0) throw std::runtime_error("Tracker request timed out; outcome unknown. Use retry before another command");
                const int index = (preferred_ + offset) % 2;
                try {
                    auto response = Response::parse(rpc(trackers_[index], message, static_cast<int>(std::min<long long>(6500, remaining))));
                    if (response.status == "RETRY_LATER") continue;
                    preferred_ = index;
                    return response;
                } catch (const std::exception&) {}
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
        }
        throw std::runtime_error("Trackers unavailable or busy; outcome unknown. Use retry (same request ID) before another command");
    }

    void Client::print(const Response& response) {
        std::cout << (response.status == "OK" ? "OK" : "ERROR " + response.status) << ": " << response.message;
        if (response.mode == "degraded") std::cout << " [degraded: pending tracker synchronization]";
        std::cout << '\n';
        for (const auto& item : response.items) std::cout << item << '\n';
        if (response.status == "OK" && (response.message == "Groups" || response.message == "Pending requests") && response.items.empty())
            std::cout << "(none)\n";
        std::cout.flush();
    }

    void Client::run() {
        ExitSignals signals;
        CommandInput input;
        const bool interactive = ::isatty(STDIN_FILENO);
        const std::map<std::string, std::size_t> arities{{"create_user", 2}, {"login", 2}, {"logout", 0},
            {"create_group", 1}, {"join_group", 1}, {"leave_group", 1}, {"list_groups", 0},
            {"list_requests", 1}, {"accept_request", 2}, {"upload_file", 2}, {"list_files", 1}, {"stop_share", 2}};
        if (interactive) std::cout << "P2P client. Type help for commands.\n";
        std::string line;
        while (true) {
            if (interactive) { std::cout << "> " << std::flush; }
            if (!input.next(line)) break;
            try {
                if (line.size() > 4096) throw std::runtime_error("Command too long");
                const auto fields = split_command(line);
                if (fields.empty()) continue;
                const auto& command = fields[0];
                if ((command == "quit" || command == "exit") && fields.size() == 1) break;
                if (command == "help" && fields.size() == 1) {
                    std::cout << "create_user <user_id> <password>\nlogin <user_id> <password>\n"
                                "create_group <group_id>\njoin_group <group_id>\nleave_group <group_id>\n"
                                "list_groups\nlist_requests <group_id>\naccept_request <group_id> <user_id>\n"
                                "upload_file <group_id> <file_path>\nlist_files <group_id>\nstop_share <group_id> <file_name>\n"
                                "download_file <group_id> <file_name> <destination_path>\nshow_downloads\n"
                                "logout\nretry\nquit\n" << std::flush;
                    continue;
                }
                if (command == "show_downloads" && fields.size() == 1) {
                    print({"OK", "Downloads", "local", "", transfers_.downloads()});
                    continue;
                }
                if (command == "download_file") {
                    if (pending_) throw std::runtime_error("Use retry to resolve the previous request first");
                    if (fields.size() != 4) throw std::runtime_error("Usage: download_file <group_id> <file_name> <destination_path>");
                    transfers_.download(fields[1], fields[2], fields[3]);
                    std::cout << "OK: Download queued\n" << std::flush;
                    continue;
                }
                if (command == "retry" && fields.size() == 1) {
                    if (!pending_) throw std::runtime_error("No uncertain request to retry");
                } else {
                    if (pending_) throw std::runtime_error("Use retry to resolve the previous request first");
                    const auto arity = arities.find(command);
                    if (arity == arities.end()) throw std::runtime_error("Unknown command; use help");
                    if (fields.size() != arity->second + 1) throw std::runtime_error("Incorrect arguments; use help");
                    Fields args(fields.begin() + 1, fields.end());
                    if (command == "login") args.push_back(endpoint_);
                    if (command == "upload_file") {
                        if (token_.empty()) throw std::runtime_error("Login before publishing a file");
                        pending_file_ = transfers_.prepare(args[0], args[1]);
                        args[1] = serialize_metadata(pending_file_->metadata);
                    }
                    if (command == "logout") transfers_.set_session("");
                    if (command == "leave_group") transfers_.leave_group(args[0]);
                    if (command == "stop_share") transfers_.stop_share(args[0], args[1]);
                    Request request{random_id(), token_, command, std::move(args)};
                    // Validate locally too, so malformed input cannot become a stuck pending request.
                    pending_ = Request::parse(request.fields());
                }
                const auto response = pending_->command == "upload_file" && pending_file_
                    ? transfers_.publish(*pending_, *pending_file_) : dispatch(*pending_);
                if (response.status == "OK" && pending_->command == "login") {
                    token_ = response.token; transfers_.set_session(token_);
                }
                if ((response.status == "OK" && pending_->command == "logout") || response.status == "UNAUTHENTICATED") { token_.clear(); transfers_.set_session(""); }
                pending_.reset(); pending_file_.reset();
                print(response);
            } catch (const std::exception& e) { std::cout << "ERROR: " << e.what() << std::endl; }
        }
        transfers_.set_session("");
        // A login may have committed even if its reply was lost. Recover its
        // token with the same request ID before attempting exit logout.
        if (pending_ && pending_->command == "login") {
            try {
                const auto response = dispatch(*pending_);
                if (response.status == "OK") token_ = response.token;
                pending_.reset();
            } catch (const std::exception&) {
                std::cerr << "Could not resolve pending login during exit; the account may remain logged in.\n";
            }
        }
        if (!token_.empty()) {
            try {
                const auto request = pending_ && pending_->command == "logout"
                    ? *pending_ : Request{random_id(), token_, "logout", {}};
                const auto response = dispatch(request);
                print(response);
                if (response.status == "OK" || response.status == "UNAUTHENTICATED") {
                    token_.clear();
                    if (pending_ && pending_->command == "logout") pending_.reset();
                } else std::cerr << "Exit logout was not confirmed; the account may remain logged in.\n";
            }
            catch (const std::exception&) { std::cerr << "Logout could not reach a tracker; the account may remain logged in.\n"; }
        }
        if (pending_) std::cerr << "Exiting with an unresolved request; its operation may have been applied.\n";
    }
} // namespace p2p
