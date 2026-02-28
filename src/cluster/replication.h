#pragma once
/// @file replication.h
/// @brief Quorum-based replication manager.

#include <string>
#include <optional>

#include "common/types.h"
#include "storage/storage_engine.h"
#include "cluster/consistent_hash.h"
#include "cluster/membership.h"

namespace kvstore {

/// Result of a quorum write (PUT or DELETE).
struct WriteResult {
    bool        success = false;
    int         acks    = 0;
    std::string error;
};

/// Result of a quorum read.
struct ReadResult {
    bool                         success = false;
    std::optional<VersionedValue> value;
    int                          responses = 0;
    std::string                  error;
};

/// Orchestrates quorum reads/writes across replica nodes.
///
/// Invariant:  R + W > N  ⟹  strong consistency.
/// Default:    N=3, R=2, W=2.
class ReplicationManager {
public:
    ReplicationManager(const std::string& self_id,
                       StorageEngine& storage,
                       ConsistentHashRing& ring,
                       MembershipManager& membership,
                       int N, int R, int W);

    WriteResult  ReplicatedPut(const std::string& key, const std::string& value);
    ReadResult   ReplicatedGet(const std::string& key);
    WriteResult  ReplicatedDelete(const std::string& key);

private:
    std::string          self_id_;
    StorageEngine&       storage_;
    ConsistentHashRing&  ring_;
    MembershipManager&   membership_;
    int N_, R_, W_;
};

}  // namespace kvstore
