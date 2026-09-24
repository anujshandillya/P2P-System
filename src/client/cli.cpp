#include "client/cli.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace p2p {
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
        : endpoint_(std::move(endpoint)), trackers_(std::move(trackers)), preferred_(preferred) {}

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
        const bool interactive = ::isatty(STDIN_FILENO);
        const std::map<std::string, std::size_t> arities{{"create_user", 2}, {"login", 2}, {"logout", 0},
            {"create_group", 1}, {"join_group", 1}, {"leave_group", 1}, {"list_groups", 0},
            {"list_requests", 1}, {"accept_request", 2}};
        if (interactive) std::cout << "P2P interim client. Type help for commands.\n";
        std::string line;
        while (true) {
            if (interactive) { std::cout << "> " << std::flush; }
            if (!std::getline(std::cin, line)) break;
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
                                "logout\nretry\nquit\n" << std::flush;
                    continue;
                }
                if (command == "retry" && fields.size() == 1) {
                    if (!pending_) throw std::runtime_error("No uncertain request to retry");
                } else {
                    if (pending_) throw std::runtime_error("Use retry to resolve the previous request first");
                    const auto arity = arities.find(command);
                    if (arity == arities.end()) throw std::runtime_error("Unknown interim command; use help");
                    if (fields.size() != arity->second + 1) throw std::runtime_error("Incorrect arguments; use help");
                    Fields args(fields.begin() + 1, fields.end());
                    if (command == "login") args.push_back(endpoint_);
                    Request request{random_id(), token_, command, std::move(args)};
                    // Validate locally too, so malformed input cannot become a stuck pending request.
                    pending_ = Request::parse(request.fields());
                }
                const auto response = dispatch(*pending_);
                if (response.status == "OK" && pending_->command == "login") token_ = response.token;
                if ((response.status == "OK" && pending_->command == "logout") || response.status == "UNAUTHENTICATED") token_.clear();
                pending_.reset();
                print(response);
            } catch (const std::exception& e) { std::cout << "ERROR: " << e.what() << std::endl; }
        }
        if (!token_.empty()) {
            try { print(dispatch(Request{random_id(), token_, "logout", {}})); }
            catch (const std::exception&) { std::cerr << "Logout could not reach a tracker; next credential login replaces this session.\n"; }
        }
        if (pending_) std::cerr << "Exiting with an unresolved request; its operation may have been applied.\n";
    }
} // namespace p2p
