#pragma once
/// @file wal.h
/// @brief Write-Ahead Log for crash-recovery durability.
///
/// Disk format per entry:
///   [4B entry_size][1B op_type][8B timestamp]
///   [4B key_len][key_bytes][4B val_len][val_bytes]
///   [4B CRC32 of everything above]

#include <string>
#include <vector>
#include <cstdint>
#include <mutex>

#include "common/types.h"

namespace kvstore {

/// Represents one WAL entry (in-memory).
struct WALEntry {
    OpType      op;
    Timestamp   timestamp;
    std::string key;
    std::string value;   // Empty for DELETE
};

/// Append-only, crash-safe write-ahead log.
class WriteAheadLog {
public:
    /// Opens (or creates) the WAL file at `filepath`.
    explicit WriteAheadLog(const std::string& filepath);
    ~WriteAheadLog();

    // Non-copyable
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    /// Append an operation record.  Thread-safe.
    void Append(OpType op, const std::string& key,
                const std::string& value, Timestamp ts);

    /// Replay all valid entries from the beginning.
    /// Stops at the first corrupt entry (partial write).
    std::vector<WALEntry> Replay();

    /// Truncate the log (e.g. after compaction snapshot).
    void Truncate();

    /// Force fsync to disk.
    void Sync();

    /// Current file size in bytes.
    size_t FileSize() const;

private:
    static uint32_t ComputeCRC(const std::vector<uint8_t>& data);

    std::string filepath_;
    int         fd_ = -1;
    std::mutex  mutex_;
};

}  // namespace kvstore
