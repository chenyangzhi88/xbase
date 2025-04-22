#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <unistd.h>
#include "resp/resp/all.hpp"
#include "xxhash.h"

namespace rangedb {
constexpr uint64_t SLICE_HEADER_SIZE = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 3 + 1;
struct ByteKey {
    uint32_t length_;
    int64_t hash_0_;
    int8_t data_[64];

    ByteKey() : length_(0), hash_0_(0) { std::memset(data_, 0, 64); }
    ByteKey(const ByteKey &other) = default;
    ByteKey &operator=(const ByteKey &other) = default;
    ByteKey(const int8_t *d, uint32_t n) : length_(n) {
        std::memset(data_, 0, 64);
        if (d == nullptr) {
            hash_0_ = 1;
        } else {
            std::memcpy(data_, d, n);
            hash_0_ = XXH64(data_, n, 0);
        }
    }

    inline void Clear() {
        std::memset(data_, 0, length_);
        length_ = 0;
    }

    inline uint32_t Size() const { return length_ + 4 + 8; }
    bool operator<(const ByteKey &other) const;
    bool operator>(const ByteKey &other) const;
    bool operator==(const ByteKey &other) const;
    bool operator<=(const ByteKey &other) const;
    bool operator>=(const ByteKey &other) const;
    bool operator!=(const ByteKey &other) const;
    int Compare(const ByteKey &other) const;
    inline void Copy(const ByteKey &other) {
        std::memcpy(const_cast<int8_t *>(data_), other.data_, other.length_);
        length_ = other.length_;
    }

    std::string ToString() const {
        if (length_ == 0) {
            return "MIN_BYTE";
        }
        if (length_ == 65) {
            return "MAX_BYTE";
        }
        std::string str((char *)data_, length_);
        return str;
    }
};
static const ByteKey MIN_BYTE = ByteKey();
static const ByteKey MAX_BYTE = ByteKey(nullptr, 65);

struct Slice {
    uint32_t data_length_;
    uint64_t version_;
    uint64_t file_id_;
    uint32_t block_id_;
    uint8_t block_type_;
    uint64_t offset_;
    ByteKey key_;
    uint8_t value_frame_;
    resp::buffer data_;
    Slice() : version_(0), offset_(0), key_(), data_length_(0), data_("", 0) {}
    void Serialize(int8_t *buffer) const;
    void Deserialize(int8_t *buffer);
    inline size_t Size() const { return SLICE_HEADER_SIZE + key_.Size() + data_.size(); }
    std::string Print() const {
        char buffer[1024];
        std::snprintf(buffer, sizeof(buffer), "key: %s offset: %ld version: %ld data_length: %u value: %s", key_.ToString().c_str(),
                      offset_, version_, data_length_, std::string(data_.data(), data_.size()).c_str());
        return std::string(buffer);
    }
};

} // namespace rangedb
