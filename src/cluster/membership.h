#pragma once
/// @file membership.h
/// @brief Gossip-based cluster membership and failure detection.

#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <functional>
#include <thread>
#include <atomic>

#include "common/types.h"
#include "common/protocol.h"

namespace kvstore {

/// Manages cluster membership using a gossip protocol.
///
/// Periodically sends heartbeats to random peers.  If a node's heartbeat
/// hasn't been refreshed within the failure-detection timeout it is marked
/// dead and the `on_leave` callback fires.
class MembershipManager {
public:
    using OnJoinCallback  = std::function<void(const NodeInfo&)>;
    using OnLeaveCallback = std::function<void(const std::string&)>;

    explicit MembershipManager(const NodeInfo& self);
    ~MembershipManager();

    // Non-copyable
    MembershipManager(const MembershipManager&) = delete;
    MembershipManager& operator=(const MembershipManager&) = delete;

    // ── Lifecycle ─────────────────────────────────
    void Start();
    void Stop();

    // ── Seeds (bootstrap) ─────────────────────────
    void AddSeed(const std::string& host, uint16_t port);

    // ── Membership ────────────────────────────────
    void AddMember(const NodeInfo& node);
    void RemoveMember(const std::string& node_id);

    std::vector<NodeInfo> GetAliveMembers() const;
    std::vector<NodeInfo> GetAllMembers() const;
    std::optional<NodeInfo> GetMember(const std::string& node_id) const;

    // ── Gossip ────────────────────────────────────
    ByteBuffer CreateGossipMessage() const;
    void HandleGossipMessage(ByteBuffer& msg);

    // ── Callbacks ─────────────────────────────────
    void SetOnJoin(OnJoinCallback cb)   { on_join_  = std::move(cb); }
    void SetOnLeave(OnLeaveCallback cb) { on_leave_ = std::move(cb); }

    const NodeInfo& Self() const { return self_; }

private:
    void GossipLoop();
    void FailureDetectionLoop();
    void ContactSeeds();

    NodeInfo self_;

    mutable std::shared_mutex members_mutex_;
    std::unordered_map<std::string, NodeInfo> members_;

    std::vector<std::pair<std::string, uint16_t>> seeds_;

    OnJoinCallback  on_join_;
    OnLeaveCallback on_leave_;

    std::atomic<bool> running_{false};
    std::thread       gossip_thread_;
    std::thread       failure_thread_;
};

}  // namespace kvstore
