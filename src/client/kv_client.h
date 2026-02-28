#pragma once
/// @file kv_client.h
/// @brief TCP client for communicating with KV store nodes.

#include <string>
#include <optional>
#include <mutex>

#include "common/protocol.h"

namespace kvstore {

/// Synchronous TCP client that speaks the KV store binary protocol.
/// Thread-safe: multiple threads can share one client instance.
class KVClient {
public:
    KVClient(const std::string& host, uint16_t port);
    ~KVClient();

    // Non-copyable
    KVClient(const KVClient&) = delete;
    KVClient& operator=(const KVClient&) = delete;

    /// Establish TCP connection (with timeout).
    bool Connect();
    void Disconnect();
    bool IsConnected() const;

    /// Send a request and receive the response.
    std::optional<ByteBuffer> SendRequest(const ByteBuffer& request);

    // ── Convenience API ──────────────────────────
    std::optional<ByteBuffer> Put(const std::string& key, const std::string& value);
    std::optional<ByteBuffer> Get(const std::string& key);
    std::optional<ByteBuffer> Delete(const std::string& key);

    std::optional<ByteBuffer> InternalPut(const std::string& key,
                                           const std::string& value,
                                           Timestamp ts,
                                           const std::string& origin);
    std::optional<ByteBuffer> InternalGet(const std::string& key);
    std::optional<ByteBuffer> InternalDelete(const std::string& key, Timestamp ts);

    std::optional<ByteBuffer> GetClusterInfo();
    std::optional<ByteBuffer> SendGossip(const ByteBuffer& gossip_payload);

private:
    std::string host_;
    uint16_t    port_;
    int         fd_ = -1;
    std::mutex  mutex_;
};

}  // namespace kvstore
