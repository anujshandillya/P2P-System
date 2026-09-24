#pragma once

#include "tracker/state.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>

namespace p2p {
    class TrackerServer {
    public:
        TrackerServer(int id, std::array<Endpoint, 2> endpoints, const std::string& journal, std::string cluster_key);
        void run();
    private:
        bool synchronize();
        Fields handle(const Fields& message);
        void worker();
        void replication_loop();
        void stop();
        int id_;
        std::array<Endpoint, 2> endpoints_;
        State state_;
        std::string key_;
        Fd listener_;
        std::atomic<bool> stopping_{false};
        std::atomic<bool> connected_{false};
        std::mutex operation_mutex_;
        std::mutex queue_mutex_;
        std::condition_variable wake_;
        std::deque<Fd> queue_;
        std::vector<std::thread> workers_;
        std::thread replicator_;
    };
} // namespace p2p
