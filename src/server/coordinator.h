#pragma once
/// @file coordinator.h
/// @brief Request router and quorum orchestrator.

#include "common/protocol.h"
#include "storage/storage_engine.h"
#include "cluster/consistent_hash.h"
#include "cluster/membership.h"
#include "cluster/replication.h"

namespace kvstore {

/// Central coordinator on each node — routes requests and enforces quorum.
///
/// External client requests go through quorum replication.
/// Internal (inter-node) requests are applied directly to local storage.
class Coordinator {
public:
    Coordinator(const std::string& self_id,
                StorageEngine& storage,
                ConsistentHashRing& ring,
                MembershipManager& membership,
                int N, int R, int W);

    /// Dispatch an incoming request based on OpType.
    ByteBuffer HandleRequest(ByteBuffer& request);

private:
    // ── Client-facing (quorum) ────────────────────
    ByteBuffer HandlePut(ByteBuffer& payload);
    ByteBuffer HandleGet(ByteBuffer& payload);
    ByteBuffer HandleDelete(ByteBuffer& payload);

    // ── Internal (replica direct) ─────────────────
    ByteBuffer HandleInternalPut(ByteBuffer& payload);
    ByteBuffer HandleInternalGet(ByteBuffer& payload);
    ByteBuffer HandleInternalDelete(ByteBuffer& payload);

    // ── Cluster ───────────────────────────────────
    ByteBuffer HandleClusterInfo();
    ByteBuffer HandleGossip(ByteBuffer& payload);

    std::string          self_id_;
    StorageEngine&       storage_;
    ConsistentHashRing&  ring_;
    MembershipManager&   membership_;
    ReplicationManager   replication_;
};

}  // namespace kvstore
