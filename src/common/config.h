#pragma once
/// @file config.h
/// @brief Compile-time configuration constants for the distributed KV store.

#include <cstdint>
#include <cstddef>

namespace kvstore {
namespace config {

// ── Cluster ────────────────────────────────────────────
constexpr int DEFAULT_REPLICATION_FACTOR = 3;   // N — number of replicas
constexpr int DEFAULT_READ_QUORUM       = 2;    // R — min reads for quorum
constexpr int DEFAULT_WRITE_QUORUM      = 2;    // W — min writes for quorum
constexpr int VIRTUAL_NODES_PER_NODE    = 150;  // Virtual nodes per physical

// ── Networking ─────────────────────────────────────────
constexpr uint16_t DEFAULT_PORT         = 7000;
constexpr int THREAD_POOL_SIZE          = 8;
constexpr int MAX_MESSAGE_SIZE          = 64 * 1024 * 1024;  // 64 MB
constexpr int CONNECTION_TIMEOUT_SEC    = 5;
constexpr int SOCKET_BACKLOG            = 128;

// ── Storage ────────────────────────────────────────────
constexpr size_t WAL_MAX_SIZE_BYTES     = 64 * 1024 * 1024;  // 64 MB

// ── Gossip / Membership ────────────────────────────────
constexpr int GOSSIP_INTERVAL_MS        = 1000;
constexpr int FAILURE_TIMEOUT_MS        = 5000;
constexpr int GOSSIP_FANOUT             = 2;   // Peers contacted per round

}  // namespace config
}  // namespace kvstore
