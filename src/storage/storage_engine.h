#pragma once
/// @file storage_engine.h
/// @brief Thread-safe in-memory key-value store backed by a WAL.

#include <string>
#include <vector>
#include <optional>
#include <unordered_map>
#include <shared_mutex>
#include <memory>

#include "common/types.h"
#include "storage/wal.h"

namespace kvstore {

/// Core storage engine: concurrent hash-map + write-ahead log.
///
/// Uses a reader-writer lock (shared_mutex) so that reads are parallel
/// and writes are exclusive — matching Amazon's read-heavy workload pattern.
class StorageEngine {
public:
    /// @param data_dir  Directory for WAL and data files.
    explicit StorageEngine(const std::string& data_dir);
    ~StorageEngine() = default;

    // Non-copyable
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    // ── Core API ──────────────────────────────────
    bool                        Put(const std::string& key,
                                    const std::string& value,
                                    Timestamp ts,
                                    const std::string& origin_node);
    std::optional<VersionedValue> Get(const std::string& key) const;
    bool                        Delete(const std::string& key, Timestamp ts);

    // ── Internal replication API ──────────────────
    /// Put that only succeeds if `ts` is newer than existing value.
    bool ConditionalPut(const std::string& key, const VersionedValue& vv);

    // ── Bulk operations (for rebalancing) ─────────
    std::vector<std::pair<std::string, VersionedValue>> GetAllData() const;
    void BulkPut(const std::vector<std::pair<std::string, VersionedValue>>& entries);
    void RemoveKeys(const std::vector<std::string>& keys);

    // ── Recovery ──────────────────────────────────
    /// Replay the WAL to restore in-memory state after crash.
    void Recover();

    // ── Metrics ───────────────────────────────────
    size_t Size() const;

private:
    mutable std::shared_mutex                          mutex_;
    std::unordered_map<std::string, VersionedValue>    store_;
    std::unique_ptr<WriteAheadLog>                     wal_;
    std::string                                        data_dir_;
};

}  // namespace kvstore
