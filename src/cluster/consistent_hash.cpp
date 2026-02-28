/// @file consistent_hash.cpp
/// @brief Consistent hash ring implementation.

#include "cluster/consistent_hash.h"
#include "common/murmurhash3.h"
#include "common/logger.h"

namespace kvstore {

ConsistentHashRing::ConsistentHashRing(int virtual_nodes)
    : virtual_nodes_(virtual_nodes) {}

// ═══════════════════════════════════════════════════════
//  Membership
// ═══════════════════════════════════════════════════════

void ConsistentHashRing::AddNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (physical_nodes_.count(node_id)) return;  // Already present
    physical_nodes_.insert(node_id);

    for (int i = 0; i < virtual_nodes_; ++i) {
        uint32_t hash = Hash(VnodeKey(node_id, i));
        ring_[hash] = node_id;
    }

    LOG_INFO("HashRing: added node '", node_id,
             "' (", virtual_nodes_, " vnodes, ring size=", ring_.size(), ")");
}

void ConsistentHashRing::RemoveNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!physical_nodes_.count(node_id)) return;
    physical_nodes_.erase(node_id);

    for (int i = 0; i < virtual_nodes_; ++i) {
        uint32_t hash = Hash(VnodeKey(node_id, i));
        ring_.erase(hash);
    }

    LOG_INFO("HashRing: removed node '", node_id,
             "' (ring size=", ring_.size(), ")");
}

bool ConsistentHashRing::HasNode(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return physical_nodes_.count(node_id) > 0;
}

// ═══════════════════════════════════════════════════════
//  Key Routing
// ═══════════════════════════════════════════════════════

std::string ConsistentHashRing::GetPrimaryNode(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty())
        throw std::runtime_error("HashRing: empty ring — no nodes available");

    uint32_t hash = Hash(key);
    auto it = ring_.upper_bound(hash);
    if (it == ring_.end()) it = ring_.begin();  // Wrap around
    return it->second;
}

std::vector<std::string> ConsistentHashRing::GetNodes(
    const std::string& key, int count) const {

    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty())
        throw std::runtime_error("HashRing: empty ring — no nodes available");

    // Clamp count to available physical nodes
    int available = static_cast<int>(physical_nodes_.size());
    if (count > available) count = available;

    uint32_t hash = Hash(key);
    auto it = ring_.upper_bound(hash);

    std::vector<std::string> result;
    std::set<std::string> seen;

    // Walk clockwise until we have `count` distinct physical nodes
    size_t steps = 0;
    while (static_cast<int>(result.size()) < count &&
           steps < ring_.size()) {
        if (it == ring_.end()) it = ring_.begin();
        if (!seen.count(it->second)) {
            seen.insert(it->second);
            result.push_back(it->second);
        }
        ++it;
        ++steps;
    }

    return result;
}

uint32_t ConsistentHashRing::HashKey(const std::string& key) const {
    return Hash(key);
}

// ═══════════════════════════════════════════════════════
//  Introspection
// ═══════════════════════════════════════════════════════

size_t ConsistentHashRing::NodeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return physical_nodes_.size();
}

size_t ConsistentHashRing::RingSize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ring_.size();
}

std::set<std::string> ConsistentHashRing::GetAllNodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return physical_nodes_;
}

// ═══════════════════════════════════════════════════════
//  Internal
// ═══════════════════════════════════════════════════════

std::string ConsistentHashRing::VnodeKey(const std::string& node_id, int index) {
    return node_id + "#" + std::to_string(index);
}

}  // namespace kvstore
