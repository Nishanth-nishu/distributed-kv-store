/// @file replication.cpp
/// @brief Quorum-based replication with parallel async writes and read repair.

#include "cluster/replication.h"
#include "client/kv_client.h"
#include "common/logger.h"

#include <future>
#include <algorithm>

namespace kvstore {

ReplicationManager::ReplicationManager(
    const std::string& self_id,
    StorageEngine& storage,
    ConsistentHashRing& ring,
    MembershipManager& membership,
    int N, int R, int W)
    : self_id_(self_id)
    , storage_(storage)
    , ring_(ring)
    , membership_(membership)
    , N_(N), R_(R), W_(W) {}

// ═══════════════════════════════════════════════════════
//  Quorum PUT
// ═══════════════════════════════════════════════════════

WriteResult ReplicationManager::ReplicatedPut(
    const std::string& key, const std::string& value) {

    WriteResult result;
    Timestamp ts = NowMs();

    // Determine replica nodes
    auto nodes = ring_.GetNodes(key, N_);
    if (nodes.empty()) {
        result.error = "No nodes available";
        return result;
    }

    // Launch writes in parallel
    std::vector<std::future<bool>> futures;

    for (const auto& node_id : nodes) {
        if (node_id == self_id_) {
            // Local write
            futures.push_back(std::async(std::launch::async, [&]() {
                return storage_.Put(key, value, ts, self_id_);
            }));
        } else {
            // Remote write
            auto member = membership_.GetMember(node_id);
            if (!member || !member->is_alive) {
                futures.push_back(std::async(std::launch::async, []() {
                    return false;
                }));
                continue;
            }
            auto host = member->host;
            auto port = member->port;
            futures.push_back(std::async(std::launch::async,
                [host, port, &key, &value, ts, sid = self_id_]() -> bool {
                    KVClient client(host, port);
                    if (!client.Connect()) return false;
                    auto resp = client.InternalPut(key, value, ts, sid);
                    if (!resp) return false;
                    auto status = static_cast<StatusCode>(resp->ReadUint8());
                    return status == StatusCode::OK;
                }));
        }
    }

    // Collect results — wait for W acks
    for (auto& fut : futures) {
        try {
            if (fut.get()) result.acks++;
        } catch (const std::exception& e) {
            LOG_WARN("Replication PUT error: ", e.what());
        }
    }

    result.success = (result.acks >= W_);
    if (!result.success) {
        result.error = "Quorum not reached: " + std::to_string(result.acks) +
                       "/" + std::to_string(W_) + " acks";
        LOG_WARN("PUT quorum failed for key '", key, "': ", result.error);
    }

    return result;
}

// ═══════════════════════════════════════════════════════
//  Quorum GET (with read repair)
// ═══════════════════════════════════════════════════════

ReadResult ReplicationManager::ReplicatedGet(const std::string& key) {
    ReadResult result;

    auto nodes = ring_.GetNodes(key, N_);
    if (nodes.empty()) {
        result.error = "No nodes available";
        return result;
    }

    // Launch reads in parallel
    struct ReadResponse {
        bool                         ok = false;
        std::optional<VersionedValue> value;
        std::string                  node_id;
    };

    std::vector<std::future<ReadResponse>> futures;

    for (const auto& node_id : nodes) {
        if (node_id == self_id_) {
            futures.push_back(std::async(std::launch::async, [&]() {
                ReadResponse r;
                r.ok = true;
                r.value = storage_.Get(key);
                r.node_id = self_id_;
                return r;
            }));
        } else {
            auto member = membership_.GetMember(node_id);
            if (!member || !member->is_alive) {
                futures.push_back(std::async(std::launch::async,
                    [nid = node_id]() {
                        return ReadResponse{false, std::nullopt, nid};
                    }));
                continue;
            }
            auto host = member->host;
            auto port = member->port;
            futures.push_back(std::async(std::launch::async,
                [host, port, &key, nid = node_id]() -> ReadResponse {
                    ReadResponse r;
                    r.node_id = nid;
                    KVClient client(host, port);
                    if (!client.Connect()) return r;
                    auto resp = client.InternalGet(key);
                    if (!resp) return r;
                    auto status = static_cast<StatusCode>(resp->ReadUint8());
                    r.ok = true;
                    if (status == StatusCode::OK) {
                        VersionedValue vv;
                        vv.value       = resp->ReadString();
                        vv.timestamp   = resp->ReadUint64();
                        vv.origin_node = resp->ReadString();
                        r.value = vv;
                    }
                    return r;
                }));
        }
    }

    // Collect and find the latest version
    std::vector<ReadResponse> responses;
    for (auto& fut : futures) {
        try {
            auto resp = fut.get();
            if (resp.ok) {
                result.responses++;
                responses.push_back(std::move(resp));
            }
        } catch (const std::exception& e) {
            LOG_WARN("Replication GET error: ", e.what());
        }
    }

    if (result.responses < R_) {
        result.error = "Read quorum not reached: " +
                       std::to_string(result.responses) + "/" +
                       std::to_string(R_);
        return result;
    }

    // Find the latest version
    VersionedValue latest;
    latest.timestamp = 0;
    bool found = false;

    for (const auto& resp : responses) {
        if (resp.value && resp.value->timestamp > latest.timestamp) {
            latest = *resp.value;
            found = true;
        }
    }

    result.success = true;
    if (found) {
        result.value = latest;

        // ── Read repair: update stale replicas asynchronously ──
        for (const auto& resp : responses) {
            if (!resp.value ||
                resp.value->timestamp < latest.timestamp) {
                if (resp.node_id == self_id_) {
                    storage_.ConditionalPut(key, latest);
                } else {
                    auto member = membership_.GetMember(resp.node_id);
                    if (member && member->is_alive) {
                        auto h = member->host;
                        auto p = member->port;
                        // Fire-and-forget repair
                        std::thread([h, p, key, latest]() {
                            KVClient c(h, p);
                            if (c.Connect()) {
                                c.InternalPut(key, latest.value,
                                              latest.timestamp,
                                              latest.origin_node);
                            }
                        }).detach();
                    }
                }
            }
        }
    }

    return result;
}

// ═══════════════════════════════════════════════════════
//  Quorum DELETE
// ═══════════════════════════════════════════════════════

WriteResult ReplicationManager::ReplicatedDelete(const std::string& key) {
    WriteResult result;
    Timestamp ts = NowMs();

    auto nodes = ring_.GetNodes(key, N_);
    if (nodes.empty()) {
        result.error = "No nodes available";
        return result;
    }

    std::vector<std::future<bool>> futures;

    for (const auto& node_id : nodes) {
        if (node_id == self_id_) {
            futures.push_back(std::async(std::launch::async, [&]() {
                return storage_.Delete(key, ts);
            }));
        } else {
            auto member = membership_.GetMember(node_id);
            if (!member || !member->is_alive) {
                futures.push_back(std::async(std::launch::async, []() {
                    return false;
                }));
                continue;
            }
            auto host = member->host;
            auto port = member->port;
            futures.push_back(std::async(std::launch::async,
                [host, port, &key, ts]() -> bool {
                    KVClient client(host, port);
                    if (!client.Connect()) return false;
                    auto resp = client.InternalDelete(key, ts);
                    if (!resp) return false;
                    auto status = static_cast<StatusCode>(resp->ReadUint8());
                    return status == StatusCode::OK;
                }));
        }
    }

    for (auto& fut : futures) {
        try {
            if (fut.get()) result.acks++;
        } catch (...) {}
    }

    result.success = (result.acks >= W_);
    if (!result.success) {
        result.error = "Delete quorum not reached";
    }

    return result;
}

}  // namespace kvstore
