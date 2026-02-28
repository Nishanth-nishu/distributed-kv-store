/// @file wal.cpp
/// @brief Write-Ahead Log implementation with CRC32 integrity checks.

#include "storage/wal.h"
#include "common/logger.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <stdexcept>

namespace kvstore {

// ═══════════════════════════════════════════════════════
//  CRC32 (Castagnoli polynomial, software fallback)
// ═══════════════════════════════════════════════════════
namespace {

uint32_t crc32_table[256];
bool     crc32_ready = false;

void InitCRC32Table() {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j)
            c = (c >> 1) ^ ((c & 1) ? 0xEDB88320u : 0);
        crc32_table[i] = c;
    }
    crc32_ready = true;
}

}  // namespace

uint32_t WriteAheadLog::ComputeCRC(const std::vector<uint8_t>& data) {
    if (!crc32_ready) InitCRC32Table();
    uint32_t crc = 0xFFFFFFFF;
    for (uint8_t b : data)
        crc = crc32_table[(crc ^ b) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFF;
}

// ═══════════════════════════════════════════════════════
//  Construction / Destruction
// ═══════════════════════════════════════════════════════

WriteAheadLog::WriteAheadLog(const std::string& filepath)
    : filepath_(filepath) {
    if (!crc32_ready) InitCRC32Table();

    fd_ = ::open(filepath.c_str(),
                 O_RDWR | O_CREAT | O_APPEND,
                 S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd_ < 0)
        throw std::runtime_error("WAL: cannot open " + filepath +
                                 ": " + std::strerror(errno));
    LOG_INFO("WAL opened: ", filepath);
}

WriteAheadLog::~WriteAheadLog() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
    }
}

// ═══════════════════════════════════════════════════════
//  Append
// ═══════════════════════════════════════════════════════

void WriteAheadLog::Append(OpType op, const std::string& key,
                           const std::string& value, Timestamp ts) {
    // Build the record (without length header and CRC)
    std::vector<uint8_t> record;

    // op (1B)
    record.push_back(static_cast<uint8_t>(op));

    // timestamp (8B big-endian)
    for (int i = 7; i >= 0; --i)
        record.push_back(static_cast<uint8_t>((ts >> (i * 8)) & 0xFF));

    // key
    uint32_t klen = static_cast<uint32_t>(key.size());
    record.push_back((klen >> 24) & 0xFF);
    record.push_back((klen >> 16) & 0xFF);
    record.push_back((klen >> 8)  & 0xFF);
    record.push_back( klen        & 0xFF);
    record.insert(record.end(), key.begin(), key.end());

    // value
    uint32_t vlen = static_cast<uint32_t>(value.size());
    record.push_back((vlen >> 24) & 0xFF);
    record.push_back((vlen >> 16) & 0xFF);
    record.push_back((vlen >> 8)  & 0xFF);
    record.push_back( vlen        & 0xFF);
    record.insert(record.end(), value.begin(), value.end());

    // CRC of the record
    uint32_t crc = ComputeCRC(record);

    // Build final on-disk blob: [4B entry_size][record][4B crc]
    uint32_t entry_size = static_cast<uint32_t>(record.size());
    std::vector<uint8_t> blob;
    blob.reserve(4 + record.size() + 4);

    blob.push_back((entry_size >> 24) & 0xFF);
    blob.push_back((entry_size >> 16) & 0xFF);
    blob.push_back((entry_size >> 8)  & 0xFF);
    blob.push_back( entry_size        & 0xFF);
    blob.insert(blob.end(), record.begin(), record.end());

    blob.push_back((crc >> 24) & 0xFF);
    blob.push_back((crc >> 16) & 0xFF);
    blob.push_back((crc >> 8)  & 0xFF);
    blob.push_back( crc        & 0xFF);

    std::lock_guard<std::mutex> lock(mutex_);
    ssize_t written = ::write(fd_, blob.data(), blob.size());
    if (written != static_cast<ssize_t>(blob.size()))
        LOG_ERROR("WAL: partial write (", written, " of ", blob.size(), ")");
    ::fdatasync(fd_);
}

// ═══════════════════════════════════════════════════════
//  Replay
// ═══════════════════════════════════════════════════════

std::vector<WALEntry> WriteAheadLog::Replay() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WALEntry> entries;

    // Seek to beginning
    ::lseek(fd_, 0, SEEK_SET);

    auto ReadBytes = [&](void* buf, size_t n) -> bool {
        size_t got = 0;
        while (got < n) {
            ssize_t r = ::read(fd_, static_cast<uint8_t*>(buf) + got, n - got);
            if (r <= 0) return false;
            got += static_cast<size_t>(r);
        }
        return true;
    };

    auto ReadU32 = [&]() -> std::pair<uint32_t, bool> {
        uint8_t b[4];
        if (!ReadBytes(b, 4)) return {0, false};
        uint32_t v = (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) |
                     (uint32_t(b[2]) << 8)  |  uint32_t(b[3]);
        return {v, true};
    };

    size_t recovered = 0;
    size_t corrupted = 0;

    while (true) {
        // Read entry size
        auto [entry_size, ok1] = ReadU32();
        if (!ok1) break;  // EOF

        // Read record bytes
        std::vector<uint8_t> record(entry_size);
        if (!ReadBytes(record.data(), entry_size)) {
            LOG_WARN("WAL: truncated record at entry ", recovered);
            ++corrupted;
            break;
        }

        // Read CRC
        auto [stored_crc, ok2] = ReadU32();
        if (!ok2) {
            LOG_WARN("WAL: truncated CRC at entry ", recovered);
            ++corrupted;
            break;
        }

        // Validate CRC
        uint32_t computed_crc = ComputeCRC(record);
        if (computed_crc != stored_crc) {
            LOG_WARN("WAL: CRC mismatch at entry ", recovered,
                     " (stored=", stored_crc, " computed=", computed_crc, ")");
            ++corrupted;
            break;
        }

        // Parse the record
        size_t pos = 0;

        if (pos >= record.size()) break;
        OpType op = static_cast<OpType>(record[pos++]);

        // timestamp
        if (pos + 8 > record.size()) break;
        Timestamp ts = 0;
        for (int i = 0; i < 8; ++i)
            ts = (ts << 8) | record[pos++];

        // key
        if (pos + 4 > record.size()) break;
        uint32_t klen = (uint32_t(record[pos]) << 24) |
                        (uint32_t(record[pos+1]) << 16) |
                        (uint32_t(record[pos+2]) << 8) |
                         uint32_t(record[pos+3]);
        pos += 4;
        if (pos + klen > record.size()) break;
        std::string key(record.begin() + pos, record.begin() + pos + klen);
        pos += klen;

        // value
        if (pos + 4 > record.size()) break;
        uint32_t vlen = (uint32_t(record[pos]) << 24) |
                        (uint32_t(record[pos+1]) << 16) |
                        (uint32_t(record[pos+2]) << 8) |
                         uint32_t(record[pos+3]);
        pos += 4;
        if (pos + vlen > record.size()) break;
        std::string value(record.begin() + pos, record.begin() + pos + vlen);
        pos += vlen;

        entries.push_back({op, ts, std::move(key), std::move(value)});
        ++recovered;
    }

    // Seek back to end for future appends
    ::lseek(fd_, 0, SEEK_END);

    LOG_INFO("WAL replay: ", recovered, " entries recovered, ",
             corrupted, " corrupted");
    return entries;
}

// ═══════════════════════════════════════════════════════
//  Utilities
// ═══════════════════════════════════════════════════════

void WriteAheadLog::Truncate() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (::ftruncate(fd_, 0) != 0)
        LOG_ERROR("WAL: truncate failed: ", std::strerror(errno));
    ::lseek(fd_, 0, SEEK_SET);
    LOG_INFO("WAL truncated");
}

void WriteAheadLog::Sync() {
    ::fdatasync(fd_);
}

size_t WriteAheadLog::FileSize() const {
    struct stat st;
    if (::fstat(fd_, &st) != 0) return 0;
    return static_cast<size_t>(st.st_size);
}

}  // namespace kvstore
