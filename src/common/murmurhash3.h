#pragma once
/// @file murmurhash3.h
/// @brief MurmurHash3 — fast, non-cryptographic hash function.
/// Based on the public-domain implementation by Austin Appleby.

#include <cstdint>
#include <string>

namespace kvstore {

/// 32-bit MurmurHash3 for x86.
uint32_t MurmurHash3_x86_32(const void* key, int len, uint32_t seed);

/// Convenience wrapper for std::string keys.
inline uint32_t Hash(const std::string& key, uint32_t seed = 0) {
    return MurmurHash3_x86_32(key.data(), static_cast<int>(key.size()), seed);
}

}  // namespace kvstore
