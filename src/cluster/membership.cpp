/// @file membership.cpp
/// @brief Gossip protocol and failure detection implementation.

#include "cluster/membership.h"
#include "client/kv_client.h"
#include "common/config.h"
#include "common/logger.h"

#include <random>
#include <algorithm>
#include <chrono>

namespace kvstore {

MembershipManager::MembershipManager(const NodeInfo& self)
    : self_(self) {
    // Add ourselves to the member list
    self_.last_heartbeat = NowMs();
    self_.is_alive = true;
    members_[self_.node_id] = self_;
}

MembershipManager::~MembershipManager() {
    Stop();
}

// ═══════════════════════════════════════════════════════
//  Lifecycle
// ═══════════════════════════════════════════════════════

void MembershipManager::Start() {
    if (running_.exchange(true)) return;

    LOG_INFO("Membership: starting gossip & failure detection");

    gossip_thread_  = std::thread(&MembershipManager::GossipLoop, this);
    failure_thread_ = std::thread(&MembershipManager::FailureDetectionLoop, this);
}

void MembershipManager::Stop() {
    if (!running_.exchange(false)) return;

    LOG_INFO("Membership: stopping");
    if (gossip_thread_.joinable())  gossip_thread_.join();
    if (failure_thread_.joinable()) failure_thread_.join();
}

// ═══════════════════════════════════════════════════════
//  Seeds
// ═══════════════════════════════════════════════════════

void MembershipManager::AddSeed(const std::string& host, uint16_t port) {
    seeds_.emplace_back(host, port);
}

// ═══════════════════════════════════════════════════════
//  Membership Management
// ═══════════════════════════════════════════════════════

void MembershipManager::AddMember(const NodeInfo& node) {
    bool is_new = false;
    {
        std::unique_lock lock(members_mutex_);
        auto it = members_.find(node.node_id);
        if (it == members_.end()) {
            members_[node.node_id] = node;
            is_new = true;
        } else {
            // Update if hearbeat is newer
            if (node.last_heartbeat > it->second.last_heartbeat) {
                it->second.last_heartbeat = node.last_heartbeat;
                if (!it->second.is_alive && node.is_alive) {
                    it->second.is_alive = true;
                    is_new = true;  // Treat revival as a join
                }
            }
        }
    }
    if (is_new && on_join_) {
        LOG_INFO("Membership: node '", node.node_id, "' joined (",
                 node.host, ":", node.port, ")");
        on_join_(node);
    }
}

void MembershipManager::RemoveMember(const std::string& node_id) {
    if (node_id == self_.node_id) return;  // Never remove self

    {
        std::unique_lock lock(members_mutex_);
        auto it = members_.find(node_id);
        if (it == members_.end()) return;
        it->second.is_alive = false;
    }

    LOG_WARN("Membership: node '", node_id, "' marked dead");
    if (on_leave_) on_leave_(node_id);
}

std::vector<NodeInfo> MembershipManager::GetAliveMembers() const {
    std::shared_lock lock(members_mutex_);
    std::vector<NodeInfo> result;
    for (const auto& [id, info] : members_) {
        if (info.is_alive) result.push_back(info);
    }
    return result;
}

std::vector<NodeInfo> MembershipManager::GetAllMembers() const {
    std::shared_lock lock(members_mutex_);
    std::vector<NodeInfo> result;
    result.reserve(members_.size());
    for (const auto& [id, info] : members_)
        result.push_back(info);
    return result;
}

std::optional<NodeInfo> MembershipManager::GetMember(
    const std::string& node_id) const {
    std::shared_lock lock(members_mutex_);
    auto it = members_.find(node_id);
    if (it == members_.end()) return std::nullopt;
    return it->second;
}

// ═══════════════════════════════════════════════════════
//  Gossip Protocol
// ═══════════════════════════════════════════════════════

ByteBuffer MembershipManager::CreateGossipMessage() const {
    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(OpType::GOSSIP));

    std::shared_lock lock(members_mutex_);
    buf.WriteUint32(static_cast<uint32_t>(members_.size()));
    for (const auto& [id, info] : members_) {
        buf.WriteString(info.node_id);
        buf.WriteString(info.host);
        buf.WriteUint16(info.port);
        buf.WriteUint64(info.last_heartbeat);
        buf.WriteBool(info.is_alive);
    }
    return buf;
}

void MembershipManager::HandleGossipMessage(ByteBuffer& msg) {
    uint32_t count = msg.ReadUint32();
    for (uint32_t i = 0; i < count; ++i) {
        NodeInfo info;
        info.node_id       = msg.ReadString();
        info.host          = msg.ReadString();
        info.port          = msg.ReadUint16();
        info.last_heartbeat = msg.ReadUint64();
        info.is_alive      = msg.ReadBool();

        if (info.node_id == self_.node_id) continue;  // Skip self

        AddMember(info);
    }
}

// ═══════════════════════════════════════════════════════
//  Background Loops
// ═══════════════════════════════════════════════════════

void MembershipManager::GossipLoop() {
    std::mt19937 rng(std::random_device{}());

    // Initial seed contact
    ContactSeeds();

    while (running_) {
        // Update own heartbeat
        {
            std::unique_lock lock(members_mutex_);
            members_[self_.node_id].last_heartbeat = NowMs();
        }

        // Select random peers to gossip with
        std::vector<NodeInfo> alive = GetAliveMembers();
        // Remove self
        alive.erase(
            std::remove_if(alive.begin(), alive.end(),
                           [&](const NodeInfo& n) {
                               return n.node_id == self_.node_id;
                           }),
            alive.end());

        if (!alive.empty()) {
            std::shuffle(alive.begin(), alive.end(), rng);
            int fanout = std::min(config::GOSSIP_FANOUT,
                                  static_cast<int>(alive.size()));

            ByteBuffer gossip_msg = CreateGossipMessage();

            for (int i = 0; i < fanout; ++i) {
                try {
                    KVClient client(alive[i].host, alive[i].port);
                    if (client.Connect()) {
                        client.SendGossip(gossip_msg);
                    }
                } catch (const std::exception& e) {
                    LOG_DEBUG("Gossip to ", alive[i].node_id, " failed: ", e.what());
                }
            }
        }

        // Sleep
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config::GOSSIP_INTERVAL_MS));
    }
}

void MembershipManager::FailureDetectionLoop() {
    while (running_) {
        Timestamp now = NowMs();
        std::vector<std::string> dead_nodes;

        {
            std::shared_lock lock(members_mutex_);
            for (const auto& [id, info] : members_) {
                if (id == self_.node_id) continue;
                if (info.is_alive &&
                    (now - info.last_heartbeat) >
                        static_cast<Timestamp>(config::FAILURE_TIMEOUT_MS)) {
                    dead_nodes.push_back(id);
                }
            }
        }

        for (const auto& id : dead_nodes) {
            RemoveMember(id);
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(config::GOSSIP_INTERVAL_MS));
    }
}

void MembershipManager::ContactSeeds() {
    for (const auto& [host, port] : seeds_) {
        try {
            KVClient client(host, port);
            if (client.Connect()) {
                // Send our gossip info to the seed
                ByteBuffer msg = CreateGossipMessage();
                auto resp = client.SendGossip(msg);
                if (resp) {
                    LOG_INFO("Membership: contacted seed ", host, ":", port);
                }
            }
        } catch (const std::exception& e) {
            LOG_WARN("Membership: seed ", host, ":", port,
                     " unreachable: ", e.what());
        }
    }
}

}  // namespace kvstore
