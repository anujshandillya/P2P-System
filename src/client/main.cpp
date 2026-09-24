#include "client/cli.hpp"
#include <cstdlib>
#include <iostream>

int main(int argc, char** argv) {
    try {
        if (argc != 3) { 
            std::cerr << "Usage: ./client <IP>:<PORT> tracker_info.txt\n"; return 1; 
        }
        
        const auto endpoint = p2p::parse_endpoint(argv[1]);
        const auto trackers = p2p::read_config(argv[2]);
        int preferred = 0;
        if (const char* value = std::getenv("P2P_PREFERRED_TRACKER")) {
            const auto id = p2p::number(value);
            if (id != 1 && id != 2) throw std::runtime_error("P2P_PREFERRED_TRACKER must be 1 or 2");
            preferred = static_cast<int>(id - 1);
        }
        p2p::ignore_sigpipe();
        // Reserve the advertised peer endpoint now; file serving is final-submission work.
        auto listener = p2p::listen_on(endpoint);
        p2p::Client client(argv[1], trackers, preferred);
        client.run();
    } catch (const std::exception& e) { 
        std::cerr << "Client error: " << e.what() << '\n'; return 1; 
    }
    
    return 0;
}
