#pragma once
/// @file consistent_hash.h
/// @brief Consistent hash ring with virtual nodes for uniform data partitioning.

#include <string>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <cstdint>

namespace kvstore {

/// Consistent hash ring using MurmurHash3.
///
/// Each physical node gets `virtual_nodes` positions on the ring,
/// ensuring balanced key distribution even with few physical nodes.
/// When a node joins/leaves, only ~1/N of the keys need to move.
class ConsistentHashRing {
public:
    /// @param virtual_nodes  Number of virtual nodes per physical node (default 150).
    explicit ConsistentHashRing(int virtual_nodes = 150);

    // ── Membership ────────────────────────────────
    void AddNode(const std::string& node_id);
    void RemoveNode(const std::string& node_id);
    bool HasNode(const std::string& node_id) const;

    // ── Key routing ───────────────────────────────
    /// Get the primary (first clockwise) node for a key.
    std::string GetPrimaryNode(const std::string& key) const;

    /// Get N distinct physical nodes responsible for a key (for replication).
    std::vector<std::string> GetNodes(const std::string& key, int count) const;

    /// Hash a key to its position on the ring.
    uint32_t HashKey(const std::string& key) const;

    // ── Introspection ─────────────────────────────
    size_t NodeCount() const;
    size_t RingSize() const;  ///< Number of virtual-node entries
    std::set<std::string> GetAllNodes() const;

private:
    int                               virtual_nodes_;
    std::map<uint32_t, std::string>   ring_;           ///< hash → node_id
    std::set<std::string>             physical_nodes_;
    mutable std::mutex                mutex_;

    /// Generate the virtual-node key for hashing.
    static std::string VnodeKey(const std::string& node_id, int index);
};

}  // namespace kvstore
