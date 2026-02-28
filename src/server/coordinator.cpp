/// @file coordinator.cpp
/// @brief Request dispatching and quorum orchestration.

#include "server/coordinator.h"
#include "common/logger.h"

namespace kvstore {

Coordinator::Coordinator(const std::string& self_id,
                         StorageEngine& storage,
                         ConsistentHashRing& ring,
                         MembershipManager& membership,
                         int N, int R, int W)
    : self_id_(self_id)
    , storage_(storage)
    , ring_(ring)
    , membership_(membership)
    , replication_(self_id, storage, ring, membership, N, R, W) {}

// ═══════════════════════════════════════════════════════
//  Request dispatcher
// ═══════════════════════════════════════════════════════

ByteBuffer Coordinator::HandleRequest(ByteBuffer& request) {
    try {
        auto op = static_cast<OpType>(request.ReadUint8());

        switch (op) {
            // Client-facing
            case OpType::PUT:             return HandlePut(request);
            case OpType::GET:             return HandleGet(request);
            case OpType::DELETE_OP:       return HandleDelete(request);

            // Internal replication
            case OpType::INTERNAL_PUT:    return HandleInternalPut(request);
            case OpType::INTERNAL_GET:    return HandleInternalGet(request);
            case OpType::INTERNAL_DELETE: return HandleInternalDelete(request);

            // Cluster
            case OpType::CLUSTER_INFO:    return HandleClusterInfo();
            case OpType::GOSSIP:          return HandleGossip(request);

            default:
                return MakeErrorResponse("Unknown operation");
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Coordinator: exception handling request: ", e.what());
        return MakeErrorResponse(std::string("Internal error: ") + e.what());
    }
}

// ═══════════════════════════════════════════════════════
//  Client-facing handlers (go through quorum)
// ═══════════════════════════════════════════════════════

ByteBuffer Coordinator::HandlePut(ByteBuffer& payload) {
    std::string key   = payload.ReadString();
    std::string value = payload.ReadString();

    LOG_DEBUG("PUT key='", key, "' value_size=", value.size());

    auto result = replication_.ReplicatedPut(key, value);
    if (result.success) {
        return MakeOkResponse();
    }
    return MakeErrorResponse(result.error);
}

ByteBuffer Coordinator::HandleGet(ByteBuffer& payload) {
    std::string key = payload.ReadString();

    LOG_DEBUG("GET key='", key, "'");

    auto result = replication_.ReplicatedGet(key);
    if (!result.success) {
        return MakeErrorResponse(result.error);
    }
    if (!result.value) {
        return MakeNotFoundResponse();
    }
    return MakeValueResponse(*result.value);
}

ByteBuffer Coordinator::HandleDelete(ByteBuffer& payload) {
    std::string key = payload.ReadString();

    LOG_DEBUG("DELETE key='", key, "'");

    auto result = replication_.ReplicatedDelete(key);
    if (result.success) {
        return MakeOkResponse();
    }
    return MakeErrorResponse(result.error);
}

// ═══════════════════════════════════════════════════════
//  Internal handlers (direct local storage, no quorum)
// ═══════════════════════════════════════════════════════

ByteBuffer Coordinator::HandleInternalPut(ByteBuffer& payload) {
    std::string key    = payload.ReadString();
    std::string value  = payload.ReadString();
    Timestamp ts       = payload.ReadUint64();
    std::string origin = payload.ReadString();

    VersionedValue vv{value, ts, origin};
    storage_.ConditionalPut(key, vv);
    return MakeOkResponse();
}

ByteBuffer Coordinator::HandleInternalGet(ByteBuffer& payload) {
    std::string key = payload.ReadString();
    auto val = storage_.Get(key);
    if (!val) return MakeNotFoundResponse();
    return MakeValueResponse(*val);
}

ByteBuffer Coordinator::HandleInternalDelete(ByteBuffer& payload) {
    std::string key = payload.ReadString();
    Timestamp ts    = payload.ReadUint64();
    storage_.Delete(key, ts);
    return MakeOkResponse();
}

// ═══════════════════════════════════════════════════════
//  Cluster handlers
// ═══════════════════════════════════════════════════════

ByteBuffer Coordinator::HandleClusterInfo() {
    auto members = membership_.GetAllMembers();

    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(StatusCode::OK));
    buf.WriteUint32(static_cast<uint32_t>(members.size()));

    for (const auto& m : members) {
        buf.WriteString(m.node_id);
        buf.WriteString(m.host);
        buf.WriteUint16(m.port);
        buf.WriteBool(m.is_alive);
    }

    // Also include local store size
    buf.WriteUint64(static_cast<uint64_t>(storage_.Size()));

    return buf;
}

ByteBuffer Coordinator::HandleGossip(ByteBuffer& payload) {
    membership_.HandleGossipMessage(payload);
    // Respond with our own gossip state
    return membership_.CreateGossipMessage();
}

}  // namespace kvstore
