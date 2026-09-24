#pragma once

#include "common/net.hpp"
#include <map>
#include <mutex>
#include <set>
#include <tuple>
#include <vector>

namespace p2p {
struct User {
    std::string password;
    std::string token;
    std::string endpoint;
};
struct Group {
    std::string owner;
    std::set<std::string> members;
    std::set<std::string> pending;
    // Current members in acceptance order; replay reconstructs this from events.
    std::vector<std::string> join_order;
};
struct Model {
    std::map<std::string, User> users;
    std::map<std::string, Group> groups;
};
struct Event {
    std::uint64_t clock;
    int origin;
    std::uint64_t sequence;
    Request request;
    std::string issued_token;
    auto order() const { return std::make_tuple(clock, origin, sequence); }
    auto identity() const { return std::make_pair(origin, sequence); }
    std::string serialize() const;
    static Event parse(const std::string& bytes);
};

// Owns the process-locked, append-only journal. No database dependency.
class Journal {
public:
    explicit Journal(const std::string& path);
    Fields load();
    void append(const Fields& records);
private:
    Fd fd_;
    bool healthy_ = true;
};

class State {
public:
    State(int tracker_id, const std::string& journal_path);
    Response execute(const Request& request);
    Fields events() const;
    void merge(const Fields& serialized);
    std::string summary() const;
private:
    using EventMap = std::map<std::pair<int, std::uint64_t>, Event>;
    using Results = std::map<std::string, std::pair<std::string, Response>>;
    static Response apply(Model& model, const Request& request, const std::string& token);
    static Results rebuild(const EventMap& events, Model& model);
    static void check_capacity(const EventMap& events);
    int id_;
    Journal journal_;
    mutable std::mutex mutex_;
    EventMap events_;
    Model model_;
    Results results_;
    std::uint64_t clock_ = 0;
    std::uint64_t sequence_ = 0;
};
} // namespace p2p
