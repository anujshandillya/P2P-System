#include "tracker/server.hpp"
#include <cstdlib>
#include <iostream>

int main(int argc, char **argv) {
    try {
        if (argc != 3) {
            std::cerr << "Usage: ./tracker tracker_info.txt <1|2>\n";
            return 1;
        }
        
        const auto id = p2p::number(argv[2]);
        if (id != 1 && id != 2) {
            throw std::runtime_error("Tracker number must be 1 or 2");
        }

        const auto endpoints = p2p::read_config(argv[1]);
        const char *journal_env = std::getenv("P2P_STATE_FILE");
        const std::string journal = journal_env ? journal_env : std::string(argv[1]) + ".tracker" + argv[2] + ".wal";
        const char *key_env = std::getenv("P2P_CLUSTER_KEY");
        const std::string key = key_env ? key_env : "p2p-interim-demo";
        
        if (key.empty() || key.size() > 256) {
            throw std::runtime_error("Invalid P2P_CLUSTER_KEY length");
        }

        p2p::ignore_sigpipe();
        p2p::TrackerServer tracker(static_cast<int>(id), endpoints, journal, key);
        tracker.run();
    } catch (const std::exception &e) {
        std::cerr << "Tracker error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
