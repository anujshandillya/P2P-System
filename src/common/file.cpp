#include "common/file.hpp"
#include "common/net.hpp"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
namespace p2p {
namespace {
void io_error(const char* operation) {
    throw std::runtime_error(std::string(operation) + ": " + std::strerror(errno));
}
bool changed(const struct stat& a, const struct stat& b) {
#ifdef __APPLE__
    const auto am = a.st_mtimespec, bm = b.st_mtimespec;
    const auto ac = a.st_ctimespec, bc = b.st_ctimespec;
#else
    const auto am = a.st_mtim, bm = b.st_mtim;
    const auto ac = a.st_ctim, bc = b.st_ctim;
#endif
    return a.st_size != b.st_size || am.tv_sec != bm.tv_sec || am.tv_nsec != bm.tv_nsec
        || ac.tv_sec != bc.tv_sec || ac.tv_nsec != bc.tv_nsec;
}
}
FileMetadata inspect_file(const std::string& path) {
    if (path.empty() || path.find('\0') != std::string::npos) throw std::runtime_error("Invalid file path");
    // Avoid blocking on a FIFO before we can reject it as nonregular.
    Fd fd(::open(path.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC));
    if (fd.get() < 0) io_error("Cannot open file");
    struct stat before{}, after{};
    if (::fstat(fd.get(), &before) < 0) io_error("Cannot inspect file");
    if (!S_ISREG(before.st_mode)) throw std::runtime_error("Path must identify a regular file");
    if (before.st_size < 0 || static_cast<std::uint64_t>(before.st_size) > max_file_size)
        throw std::runtime_error("File exceeds the 1 GiB limit");
    FileMetadata metadata;
    const auto slash = path.find_last_of('/');
    metadata.name = path.substr(slash == std::string::npos ? 0 : slash + 1);
    metadata.size = static_cast<std::uint64_t>(before.st_size);
    metadata.piece_hashes.reserve((metadata.size + piece_size - 1) / piece_size);
    std::vector<std::uint8_t> buffer(piece_size);
    Sha1 whole;
    std::uint64_t remaining = metadata.size;
    while (remaining) {
        const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, piece_size));
        std::size_t received = 0;
        while (received < length) {
            const auto count = ::read(fd.get(), buffer.data() + received, length - received);
            if (count < 0) { if (errno == EINTR) continue; io_error("Cannot read file"); }
            if (count == 0) throw std::runtime_error("File changed during inspection (unexpected EOF)");
            received += static_cast<std::size_t>(count);
        }
        whole.update(buffer.data(), length);
        Sha1 piece;
        piece.update(buffer.data(), length);
        metadata.piece_hashes.push_back(piece.finalize());
        remaining -= length;
    }
    metadata.whole_hash = whole.finalize();
    if (::fstat(fd.get(), &after) < 0) io_error("Cannot recheck file");
    if (changed(before, after)) throw std::runtime_error("File changed during inspection; try again");
    return metadata;
}
}
