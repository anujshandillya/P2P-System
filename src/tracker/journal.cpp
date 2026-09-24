#include "tracker/state.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace p2p {
    namespace {
        constexpr std::uint32_t max_record = 8192;
        std::uint32_t checksum(const std::string& bytes) {
            std::uint32_t hash = 2166136261u;
            for (unsigned char byte : bytes) { hash ^= byte; hash *= 16777619u; }
            return hash;
        }
        void append32(std::string& bytes, std::uint32_t n) {
            for (int i = 24; i >= 0; i -= 8) bytes += static_cast<char>((n >> i) & 255);
        }
        std::uint32_t read32(const char* p) {
            std::uint32_t n = 0;
            for (int i = 0; i < 4; ++i) n = (n << 8) | static_cast<unsigned char>(p[i]);
            return n;
        }
        std::size_t read_at(int fd, char* buffer, std::size_t length, off_t offset) {
            std::size_t done = 0;
            while (done < length) {
                const auto n = ::pread(fd, buffer + done, length - done, offset + static_cast<off_t>(done));
                if (n < 0 && errno == EINTR) continue;
                if (n < 0) throw std::runtime_error("Cannot read journal");
                if (n == 0) break;
                done += static_cast<std::size_t>(n);
            }
            return done;
        }
    }

    Journal::Journal(const std::string& path)
        : fd_(::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_NOFOLLOW, 0600)) {
        if (fd_.get() < 0) throw std::runtime_error("Cannot open journal: " + std::string(std::strerror(errno)));
        struct stat info{};
        if (::fstat(fd_.get(), &info) < 0 || !S_ISREG(info.st_mode)) throw std::runtime_error("Journal must be a regular file");
        if (::fchmod(fd_.get(), 0600) < 0) throw std::runtime_error("Cannot restrict journal permissions");
        struct flock lock{};
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        if (::fcntl(fd_.get(), F_SETLK, &lock) < 0) throw std::runtime_error("Journal is already used by another tracker");
    }

    Fields Journal::load() {
        Fields records;
        off_t offset = 0;
        for (;;) {
            char header[12];
            const auto n = read_at(fd_.get(), header, sizeof(header), offset);
            if (n == 0) break;
            if (n != sizeof(header)) {
                if (::ftruncate(fd_.get(), offset) < 0 || ::fsync(fd_.get()) < 0)
                    throw std::runtime_error("Cannot repair partial journal header");
                break;
            }
            const auto length = read32(header + 4);
            if (std::memcmp(header, "WAL1", 4) != 0 || length == 0 || length > max_record)
                throw std::runtime_error("Corrupt journal header (restore a known-good journal)");
            std::string bytes(length, '\0');
            if (read_at(fd_.get(), bytes.data(), length, offset + 12) != length) {
                if (::ftruncate(fd_.get(), offset) < 0 || ::fsync(fd_.get()) < 0)
                    throw std::runtime_error("Cannot repair partial journal record");
                break;
            }
            if (checksum(bytes) != read32(header + 8)) throw std::runtime_error("Journal checksum mismatch");
            records.push_back(std::move(bytes));
            offset += 12 + length;
            if (offset > 8 * 1024 * 1024) throw std::runtime_error("Interim journal capacity exceeded");
        }
        return records;
    }

    void Journal::append(const Fields& records) {
        if (!healthy_) throw std::runtime_error("Journal requires restart/repair; writes are disabled");
        if (records.empty()) return;
        std::string batch;
        for (const auto& record : records) {
            if (record.empty() || record.size() > max_record) throw std::runtime_error("Invalid journal record size");
            batch += "WAL1";
            append32(batch, static_cast<std::uint32_t>(record.size()));
            append32(batch, checksum(record));
            batch += record;
        }
        const auto start = ::lseek(fd_.get(), 0, SEEK_END);
        if (start < 0) throw std::runtime_error("Cannot seek journal");
        std::size_t done = 0;
        while (done < batch.size()) {
            const auto n = ::write(fd_.get(), batch.data() + done, batch.size() - done);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) break;
            done += static_cast<std::size_t>(n);
        }
        if (done != batch.size() || ::fsync(fd_.get()) < 0) {
            // Restore the last acknowledged prefix before allowing another mutation.
            if (::ftruncate(fd_.get(), start) < 0 || ::fsync(fd_.get()) < 0) {
                healthy_ = false;
                throw std::runtime_error("Journal rollback failed; tracker requires restart/repair");
            }
            throw std::runtime_error("Journal write failed; operation was not acknowledged");
        }
    }
} // namespace p2p
