/// @file kv_client.cpp
/// @brief TCP client implementation for the distributed KV store.

#include "client/kv_client.h"
#include "common/logger.h"
#include "common/config.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <cstring>

namespace kvstore {

KVClient::KVClient(const std::string& host, uint16_t port)
    : host_(host), port_(port) {}

KVClient::~KVClient() {
    Disconnect();
}

bool KVClient::Connect() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ >= 0) return true;  // Already connected

    // Resolve hostname
    struct addrinfo hints{}, *result = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port_);
    int rv = ::getaddrinfo(host_.c_str(), port_str.c_str(), &hints, &result);
    if (rv != 0 || !result) {
        LOG_ERROR("Client: cannot resolve ", host_, ":", port_,
                  " — ", gai_strerror(rv));
        return false;
    }

    fd_ = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (fd_ < 0) {
        ::freeaddrinfo(result);
        LOG_ERROR("Client: socket() failed: ", std::strerror(errno));
        return false;
    }

    // Set connect timeout using non-blocking + poll
    int flags = ::fcntl(fd_, F_GETFL, 0);
    ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);

    int conn = ::connect(fd_, result->ai_addr, result->ai_addrlen);
    ::freeaddrinfo(result);

    if (conn < 0 && errno != EINPROGRESS) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    if (conn < 0) {
        // Wait for connection with timeout
        struct pollfd pfd{fd_, POLLOUT, 0};
        int pr = ::poll(&pfd, 1, config::CONNECTION_TIMEOUT_SEC * 1000);
        if (pr <= 0) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }
    }

    // Restore blocking mode
    ::fcntl(fd_, F_SETFL, flags);

    // Enable TCP_NODELAY for low latency
    int one = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    return true;
}

void KVClient::Disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool KVClient::IsConnected() const {
    return fd_ >= 0;
}

std::optional<ByteBuffer> KVClient::SendRequest(const ByteBuffer& request) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (fd_ < 0) return std::nullopt;

    if (!SendMessage(fd_, request)) {
        ::close(fd_);
        fd_ = -1;
        return std::nullopt;
    }

    auto response = RecvMessage(fd_);
    if (!response) {
        ::close(fd_);
        fd_ = -1;
    }
    return response;
}

// ═══════════════════════════════════════════════════════
//  Convenience API
// ═══════════════════════════════════════════════════════

std::optional<ByteBuffer> KVClient::Put(const std::string& key,
                                         const std::string& value) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::PUT));
    req.WriteString(key);
    req.WriteString(value);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::Get(const std::string& key) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::GET));
    req.WriteString(key);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::Delete(const std::string& key) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::DELETE_OP));
    req.WriteString(key);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::InternalPut(const std::string& key,
                                                  const std::string& value,
                                                  Timestamp ts,
                                                  const std::string& origin) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::INTERNAL_PUT));
    req.WriteString(key);
    req.WriteString(value);
    req.WriteUint64(ts);
    req.WriteString(origin);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::InternalGet(const std::string& key) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::INTERNAL_GET));
    req.WriteString(key);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::InternalDelete(const std::string& key,
                                                     Timestamp ts) {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::INTERNAL_DELETE));
    req.WriteString(key);
    req.WriteUint64(ts);
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::GetClusterInfo() {
    ByteBuffer req;
    req.WriteUint8(static_cast<uint8_t>(OpType::CLUSTER_INFO));
    return SendRequest(req);
}

std::optional<ByteBuffer> KVClient::SendGossip(const ByteBuffer& gossip_payload) {
    return SendRequest(gossip_payload);
}

}  // namespace kvstore
