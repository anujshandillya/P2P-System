#include "tracker/state.hpp"
#include <algorithm>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace p2p {
    namespace {
        Response error(const std::string& code, const std::string& message) {
            return {code, message, "local", "", {}};
        }
        bool identifier(const std::string& value) {
            if (value.empty() || value.size() > 64) return false;
            for (unsigned char c : value)
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.'))
                    return false;
            return true;
        }
        bool password(const std::string& value) {
            if (value.empty() || value.size() > 128) return false;
            for (unsigned char c : value) if (c < 32 || c > 126) return false;
            return true;
        }
        bool read_only(const std::string& command) { return command == "list_groups" || command == "list_requests"; }
    }

    std::string Event::serialize() const {
        return encode({std::to_string(clock), std::to_string(origin), std::to_string(sequence), encode(request.fields()), issued_token});
    }
    Event Event::parse(const std::string& bytes) {
        if (bytes.size() > 8192) throw std::runtime_error("Event too large");
        const auto f = decode(bytes);
        if (f.size() != 5) throw std::runtime_error("Invalid event");
        const auto origin = number(f[1]);
        const auto clock = number(f[0]);
        const auto sequence = number(f[2]);
        if ((origin != 1 && origin != 2) || clock == 0 || sequence == 0 || clock < sequence ||
            clock == std::numeric_limits<std::uint64_t>::max()) throw std::runtime_error("Invalid event identity");
        auto request = Request::parse(decode(f[3]));
        if (read_only(request.command)) throw std::runtime_error("Read operation in mutation log");
        if ((request.command == "login" && f[4].size() != 64) || (request.command != "login" && !f[4].empty()))
            throw std::runtime_error("Invalid issued token");
        return {clock, static_cast<int>(origin), sequence, std::move(request), f[4]};
    }

    Response State::apply(Model& model, const Request& req, const std::string& issued_token) {
        const auto& cmd = req.command;
        const auto& a = req.args;
        static const std::map<std::string, std::size_t> arities{
            {"create_user", 2}, {"login", 3}, {"logout", 0}, {"create_group", 1},
            {"join_group", 1}, {"leave_group", 1}, {"list_groups", 0}, {"list_requests", 1}, {"accept_request", 2}};
        const auto arity = arities.find(cmd);
        if (arity == arities.end()) return error("UNKNOWN_COMMAND", "Unknown interim command");
        if (a.size() != arity->second) return error("INVALID_ARGUMENT", "Incorrect number of command arguments");
        if (cmd == "create_user" || cmd == "login") {
            if (!identifier(a[0]) || !password(a[1])) return error("INVALID_ARGUMENT", "Invalid user ID or password");
            auto user = model.users.find(a[0]);
            if (cmd == "create_user") {
                if (user != model.users.end()) return error("ALREADY_EXISTS", "User already exists");
                model.users.emplace(a[0], User{a[1], "", ""});
                return {"OK", "User created", "local", "", {}};
            }
            try { (void)parse_endpoint(a[2]); }
            catch (const std::exception&) { return error("INVALID_ARGUMENT", "Invalid client endpoint"); }
            if (user == model.users.end() || user->second.password != a[1])
                return error("AUTH_FAILED", "Invalid user ID or password");
            if (!req.token.empty()) return error("ALREADY_LOGGED_IN", "Log out before logging in again");
            // A fresh credential login replaces a lost client's old session.
            user->second.token = issued_token;
            user->second.endpoint = a[2];
            return {"OK", "Logged in", "local", issued_token, {}};
        }

        std::string actor;
        if (!req.token.empty()) {
            for (const auto& entry : model.users)
                if (entry.second.token == req.token) { actor = entry.first; break; }
        }
        if (actor.empty()) return error("UNAUTHENTICATED", "Please log in; session is missing or revoked");
        if (cmd == "logout") {
            model.users.at(actor).token.clear();
            model.users.at(actor).endpoint.clear();
            return {"OK", "Logged out", "local", "", {}};
        }
        if (cmd == "list_groups") {
            Response response{"OK", "Groups", "local", "", {}};
            for (const auto& group : model.groups) response.items.push_back(group.first);
            return response;
        }
        if (!identifier(a[0]) || (cmd == "accept_request" && !identifier(a[1])))
            return error("INVALID_ARGUMENT", "IDs must use letters, digits, '.', '_' or '-' (1..64 characters)");
        auto group_it = model.groups.find(a[0]);
        if (cmd == "create_group") {
            if (group_it != model.groups.end()) return error("ALREADY_EXISTS", "Group already exists");
            model.groups.emplace(a[0], Group{actor, {actor}, {}, {actor}});
            return {"OK", "Group created", "local", "", {}};
        }
        if (group_it == model.groups.end()) return error("NOT_FOUND", "Group does not exist");
        auto& group = group_it->second;
        if (cmd == "join_group") {
            if (group.members.count(actor)) return error("ALREADY_MEMBER", "Already a group member");
            if (!group.pending.insert(actor).second) return error("ALREADY_PENDING", "Join request already pending");
            return {"OK", "Join request sent", "local", "", {}};
        }
        if (cmd == "leave_group") {
            if (!group.members.count(actor)) return error("NOT_MEMBER", "Not a group member");
            group.members.erase(actor);
            group.join_order.erase(std::remove(group.join_order.begin(), group.join_order.end(), actor), group.join_order.end());
            if (group.members.empty()) {
                model.groups.erase(group_it);
                return {"OK", "Last member left; group deleted", "local", "", {}};
            }
            if (group.owner == actor) {
                group.owner = group.join_order.front();
                return {"OK", "Left group; new owner: " + group.owner, "local", "", {}};
            }
            return {"OK", "Left group", "local", "", {}};
        }
        if (group.owner != actor) return error("FORBIDDEN", "Only the group owner may manage requests");
        if (cmd == "list_requests") {
            return {"OK", "Pending requests", "local", "", Fields(group.pending.begin(), group.pending.end())};
        }
        if (!group.pending.count(a[1])) return error("NOT_FOUND", "No pending request for that user");
        group.pending.erase(a[1]);
        group.members.insert(a[1]);
        group.join_order.push_back(a[1]);
        return {"OK", "Join request accepted", "local", "", {}};
    }

    State::Results State::rebuild(const EventMap& events, Model& model) {
        model = {};
        std::vector<const Event*> ordered;
        for (const auto& entry : events) ordered.push_back(&entry.second);
        std::sort(ordered.begin(), ordered.end(), [](const Event* a, const Event* b) { return a->order() < b->order(); });
        Results results;
        for (const auto* event : ordered) {
            const auto& req = event->request;
            if (results.count(req.id)) continue; // Earliest total-order occurrence wins after a partition.
            results.emplace(req.id, std::make_pair(encode(req.fields()), apply(model, req, event->issued_token)));
        }
        return results;
    }

    void State::check_capacity(const EventMap& events) {
        std::size_t bytes = 0;
        for (const auto& event : events) bytes += event.second.serialize().size() + 12;
        if (bytes > 8 * 1024 * 1024 || events.size() > 50000)
            throw std::runtime_error("Interim metadata capacity reached (8 MiB / 50000 events)");
    }

    State::State(int tracker_id, const std::string& journal_path) : id_(tracker_id), journal_(journal_path) {
        for (const auto& record : journal_.load()) {
            auto event = Event::parse(record);
            const auto existing = events_.find(event.identity());
            if (existing != events_.end() && existing->second.serialize() != record)
                throw std::runtime_error("Conflicting event IDs in journal");
            clock_ = std::max(clock_, event.clock);
            if (event.origin == id_) sequence_ = std::max(sequence_, event.sequence);
            events_.insert_or_assign(event.identity(), std::move(event));
        }
        check_capacity(events_);
        results_ = rebuild(events_, model_);
    }

    Response State::execute(const Request& request) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto previous = results_.find(request.id);
        if (previous != results_.end()) {
            if (previous->second.first != encode(request.fields()))
                return error("REQUEST_ID_CONFLICT", "Request ID was reused with different arguments");
            return previous->second.second;
        }
        Model next = model_;
        const auto token = request.command == "login" ? random_id() : "";
        auto response = apply(next, request, token);
        if (response.status != "OK" || read_only(request.command)) return response;
        if (clock_ >= std::numeric_limits<std::uint64_t>::max() - 1)
            throw std::runtime_error("Logical clock exhausted");
        Event event{clock_ + 1, id_, sequence_ + 1, request, token};
        auto next_events = events_;
        next_events.emplace(event.identity(), event);
        check_capacity(next_events);
        // The mutex also serializes journal I/O with replay. No socket I/O occurs here.
        journal_.append({event.serialize()});
        events_.swap(next_events);
        model_ = std::move(next);
        results_.emplace(request.id, std::make_pair(encode(request.fields()), response));
        clock_ = event.clock;
        sequence_ = event.sequence;
        return response;
    }

    Fields State::events() const {
        std::lock_guard<std::mutex> lock(mutex_);
        Fields out;
        for (const auto& event : events_) out.push_back(event.second.serialize());
        return out;
    }

    void State::merge(const Fields& serialized) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto next_events = events_;
        Fields added;
        for (const auto& bytes : serialized) {
            auto event = Event::parse(bytes);
            const auto old = next_events.find(event.identity());
            if (old != next_events.end()) {
                if (old->second.serialize() != bytes) throw std::runtime_error("Conflicting replicated event identity");
                continue;
            }
            next_events.emplace(event.identity(), std::move(event));
            added.push_back(bytes);
        }
        if (added.empty()) return;
        check_capacity(next_events);
        Model next_model;
        auto next_results = rebuild(next_events, next_model);
        journal_.append(added);
        events_.swap(next_events);
        model_ = std::move(next_model);
        results_ = std::move(next_results);
        for (const auto& event : events_) {
            clock_ = std::max(clock_, event.second.clock);
            if (event.second.origin == id_) sequence_ = std::max(sequence_, event.second.sequence);
        }
    }

    std::string State::summary() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::size_t conflicts = 0;
        for (const auto& result : results_) if (result.second.second.status != "OK") ++conflicts;
        std::ostringstream out;
        out << "events=" << events_.size() << " users=" << model_.users.size() << " groups=" << model_.groups.size()
            << " reconciliation_conflicts=" << conflicts;
        return out.str();
    }
} // namespace p2p
