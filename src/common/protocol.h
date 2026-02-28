#pragma once
/// @file protocol.h
/// @brief Binary wire protocol — serialization, deserialization, and TCP I/O.
///
/// Message framing:
///   [4 bytes: payload_length (network order)]
///   [payload_length bytes: payload]
///
/// Payload begins with a 1-byte OpType (requests) or StatusCode (responses),
/// followed by type-specific fields encoded with this ByteBuffer.

#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>

#include <arpa/inet.h>
#include <unistd.h>
#include <sys/socket.h>

#include "common/config.h"
#include "common/types.h"

namespace kvstore {

// ═══════════════════════════════════════════════════════
//  ByteBuffer — zero-copy serialization helper
// ═══════════════════════════════════════════════════════

class ByteBuffer {
public:
    ByteBuffer() = default;
    explicit ByteBuffer(std::vector<uint8_t> data)
        : data_(std::move(data)), read_pos_(0) {}

    // ── Writers ────────────────────────────────────
    void WriteUint8(uint8_t val) {
        data_.push_back(val);
    }
    void WriteUint16(uint16_t val) {
        uint16_t n = htons(val);
        Append(&n, 2);
    }
    void WriteUint32(uint32_t val) {
        uint32_t n = htonl(val);
        Append(&n, 4);
    }
    void WriteUint64(uint64_t val) {
        for (int i = 7; i >= 0; --i)
            data_.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
    }
    void WriteString(const std::string& s) {
        WriteUint32(static_cast<uint32_t>(s.size()));
        data_.insert(data_.end(), s.begin(), s.end());
    }
    void WriteBool(bool v) { WriteUint8(v ? 1 : 0); }

    // ── Readers ────────────────────────────────────
    uint8_t ReadUint8() {
        EnsureReadable(1);
        return data_[read_pos_++];
    }
    uint16_t ReadUint16() {
        EnsureReadable(2);
        uint16_t n;
        std::memcpy(&n, data_.data() + read_pos_, 2);
        read_pos_ += 2;
        return ntohs(n);
    }
    uint32_t ReadUint32() {
        EnsureReadable(4);
        uint32_t n;
        std::memcpy(&n, data_.data() + read_pos_, 4);
        read_pos_ += 4;
        return ntohl(n);
    }
    uint64_t ReadUint64() {
        EnsureReadable(8);
        uint64_t val = 0;
        for (int i = 0; i < 8; ++i)
            val = (val << 8) | data_[read_pos_++];
        return val;
    }
    std::string ReadString() {
        uint32_t len = ReadUint32();
        EnsureReadable(len);
        std::string s(data_.begin() + read_pos_,
                      data_.begin() + read_pos_ + len);
        read_pos_ += len;
        return s;
    }
    bool ReadBool() { return ReadUint8() != 0; }

    // ── Accessors ──────────────────────────────────
    const std::vector<uint8_t>& Data() const { return data_; }
    size_t Size()      const { return data_.size(); }
    size_t Remaining() const { return data_.size() - read_pos_; }
    void   ResetRead()       { read_pos_ = 0; }

private:
    void Append(const void* p, size_t n) {
        auto* b = static_cast<const uint8_t*>(p);
        data_.insert(data_.end(), b, b + n);
    }
    void EnsureReadable(size_t n) {
        if (read_pos_ + n > data_.size())
            throw std::runtime_error("ByteBuffer: underflow");
    }

    std::vector<uint8_t> data_;
    size_t read_pos_ = 0;
};

// ═══════════════════════════════════════════════════════
//  TCP helpers — reliable send / receive with framing
// ═══════════════════════════════════════════════════════

/// Send exactly `len` bytes.
inline bool SendAll(int fd, const void* data, size_t len) {
    auto* ptr = static_cast<const uint8_t*>(data);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, ptr + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

/// Receive exactly `len` bytes.
inline bool RecvAll(int fd, void* data, size_t len) {
    auto* ptr = static_cast<uint8_t*>(data);
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, ptr + got, len - got, 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

/// Send a length-prefixed message.
inline bool SendMessage(int fd, const ByteBuffer& buf) {
    uint32_t net_len = htonl(static_cast<uint32_t>(buf.Size()));
    if (!SendAll(fd, &net_len, 4)) return false;
    if (buf.Size() == 0) return true;
    return SendAll(fd, buf.Data().data(), buf.Size());
}

/// Receive a length-prefixed message.
inline std::optional<ByteBuffer> RecvMessage(int fd) {
    uint32_t net_len;
    if (!RecvAll(fd, &net_len, 4)) return std::nullopt;
    uint32_t len = ntohl(net_len);
    if (len > static_cast<uint32_t>(config::MAX_MESSAGE_SIZE))
        return std::nullopt;
    std::vector<uint8_t> data(len);
    if (len > 0 && !RecvAll(fd, data.data(), len))
        return std::nullopt;
    return ByteBuffer(std::move(data));
}

// ═══════════════════════════════════════════════════════
//  Convenience builders for common response messages
// ═══════════════════════════════════════════════════════

inline ByteBuffer MakeOkResponse() {
    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(StatusCode::OK));
    return buf;
}

inline ByteBuffer MakeErrorResponse(const std::string& msg) {
    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(StatusCode::ERROR));
    buf.WriteString(msg);
    return buf;
}

inline ByteBuffer MakeNotFoundResponse() {
    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(StatusCode::NOT_FOUND));
    return buf;
}

inline ByteBuffer MakeValueResponse(const VersionedValue& vv) {
    ByteBuffer buf;
    buf.WriteUint8(static_cast<uint8_t>(StatusCode::OK));
    buf.WriteString(vv.value);
    buf.WriteUint64(vv.timestamp);
    buf.WriteString(vv.origin_node);
    return buf;
}

}  // namespace kvstore
