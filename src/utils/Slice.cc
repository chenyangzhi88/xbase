#include "utils/Slice.h"
#include <cstdint>
namespace rangedb {
bool ByteKey::operator<(const ByteKey &other) const {
    if (length_ < other.length_) {
        return true;
    }
    if (other.length_ < length_) {
        return false;
    }
    return std::memcmp(data_, other.data_, length_) < 0;
}

bool ByteKey::operator==(const ByteKey &other) const {
    if (hash_0_ != other.hash_0_) {
        return false;
    }
    if (length_ != other.length_) {
        return true;
    }
    if ((length_ == 0 && other.length_ == 0) || (length_ == 65 && other.length_ == 65)) {
        return true;
    }
    if (length_ == other.length_ && std::memcmp(data_, other.data_, length_) == 0) {
        return true;
    }
    return false;
}
bool ByteKey::operator>(const ByteKey &other) const { return other < *this && !(*this == other); }

bool ByteKey::operator<=(const ByteKey &other) const { return !(other < *this); }

bool ByteKey::operator>=(const ByteKey &other) const { return !(*this < other); }

bool ByteKey::operator!=(const ByteKey &other) const { return !(*this == other); }

int ByteKey::Compare(const ByteKey &other) const {
    if (*this < other) {
        return -1;
    }
    if (*this == other) {
        return 0;
    }
    return 1;
}

void Slice::Serialize(int8_t *buffer) const {
    std::memcpy(buffer, &data_length_, sizeof(data_length_));
    buffer += sizeof(data_length_);
    std::memcpy(buffer, &version_, sizeof(version_));
    buffer += sizeof(version_);
    std::memcpy(buffer, &file_id_, sizeof(file_id_));
    buffer += sizeof(file_id_);
    std::memcpy(buffer, &block_id_, sizeof(block_id_));
    buffer += sizeof(block_id_);
    std::memcpy(buffer, &block_type_, sizeof(block_type_));
    buffer += sizeof(block_type_);
    std::memcpy(buffer, &offset_, sizeof(offset_));
    buffer += sizeof(offset_);
    std::memcpy(buffer, &key_.length_, sizeof(key_.length_));
    buffer += sizeof(key_.length_);
    std::memcpy(buffer, &key_.hash_0_, sizeof(key_.hash_0_));
    buffer += sizeof(key_.hash_0_);
    std::memcpy(buffer, key_.data_, key_.length_);
    buffer += key_.length_;
    std::memcpy(buffer, data_.data(), data_.size());
}
void Slice::Deserialize(int8_t *buffer) {
    std::memcpy(&data_length_, buffer, sizeof(data_length_));
    buffer += sizeof(data_length_);
    std::memcpy(&version_, buffer, sizeof(version_));
    buffer += sizeof(version_);
    std::memcpy(&file_id_, buffer, sizeof(file_id_));
    buffer += sizeof(file_id_);
    std::memcpy(&block_id_, buffer, sizeof(block_id_));
    buffer += sizeof(block_id_);
    std::memcpy(&block_type_, buffer, sizeof(block_type_));
    buffer += sizeof(block_type_);
    std::memcpy(&offset_, buffer, sizeof(offset_));
    buffer += sizeof(offset_);
    std::memcpy(&key_.length_, buffer, sizeof(key_.length_));
    buffer += sizeof(key_.length_);
    std::memcpy(&key_.hash_0_, buffer, sizeof(key_.hash_0_));
    buffer += sizeof(key_.hash_0_);
    std::memcpy(key_.data_, buffer, key_.length_);
    buffer += key_.length_;
    uint32_t value_size = data_length_ - SLICE_HEADER_SIZE - key_.Size();
    data_ = resp::buffer((char *)buffer, value_size);
}
} // namespace rangedb