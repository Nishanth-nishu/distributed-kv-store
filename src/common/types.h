#pragma once
/// @file types.h
/// @brief Core data types used across the distributed KV store.

#include <string>
#include <cstdint>
#include <chrono>

namespace kvstore {

/// Monotonic timestamp in milliseconds since epoch.
using Timestamp = uint64_t;

/// Returns current wall-clock time as a Timestamp.
inline Timestamp NowMs() {
    return static_cast<Timestamp>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

/// Metadata about a cluster node.
struct NodeInfo {
    std::string node_id;        ///< Unique identifier (e.g. "node1")
    std::string host;           ///< Hostname or IP
    uint16_t    port = 0;       ///< Listening port
    bool        is_alive = true;
    Timestamp   last_heartbeat = 0;

    std::string Address() const { return host + ":" + std::to_string(port); }
};

/// A value stored in the KV store, tagged with version info.
struct VersionedValue {
    std::string value;
    Timestamp   timestamp = 0;
    std::string origin_node;    ///< Node that first wrote this value
};

/// Operation types for the wire protocol.
enum class OpType : uint8_t {
    // Client operations
    PUT             = 1,
    GET             = 2,
    DELETE_OP       = 3,   // "DELETE" collides with macros on some platforms

    // Internal (inter-node) replication
    INTERNAL_PUT    = 10,
    INTERNAL_GET    = 11,
    INTERNAL_DELETE = 12,

    // Cluster administration
    JOIN_CLUSTER    = 20,
    LEAVE_CLUSTER   = 21,
    CLUSTER_INFO    = 22,

    // Data transfer during rebalancing
    TRANSFER_KEYS   = 30,

    // Gossip protocol
    GOSSIP          = 40,
};

/// Wire-level response status.
enum class StatusCode : uint8_t {
    OK          = 0,
    NOT_FOUND   = 1,
    ERROR       = 2,
};

}  // namespace kvstore
