#include "common/file.hpp"
#include <iostream>
#include <stdexcept>
int main(int argc, char** argv) {
    try {
        p2p::Sha1 split;
        split.update("a", 1); split.update("bc", 2);
        if (p2p::to_hex(split.finalize()) != "a9993e364706816aba3e25717850c26c9cd0d89d")
            throw std::runtime_error("Incremental SHA1 mismatch");
        bool rejected = false;
        try { split.finalize(); } catch (const std::logic_error&) { rejected = true; }
        if (!rejected) throw std::runtime_error("Repeated finalize accepted");
        if (argc != 2) return 2;
        const auto file = p2p::inspect_file(argv[1]);
        std::cout << file.name << '\n' << file.size << '\n' << p2p::to_hex(file.whole_hash) << '\n';
        for (const auto& hash : file.piece_hashes) std::cout << p2p::to_hex(hash) << '\n';
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
