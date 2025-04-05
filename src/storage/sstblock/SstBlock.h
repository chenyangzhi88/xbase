#pragma once

#include "storage/block/Block.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include <cstdint>
#include <cstring>
#include <memory>
#include <unistd.h>
#include <utility>

namespace rangedb {
namespace storage {
const size_t SSTBLOCK_HEAD_SIZE = 256;
class SstBlock : virtual public Block {
public:
    SstBlock(uint64_t block_id) : block_id_(block_id) {
        block_id_ = block_id;
        data_ = new int8_t[BLOCK_SIZE]{0};
        write_offset_ = SSTBLOCK_HEAD_SIZE;
        num_restarts_ = 0;
        counter_ = 0;
    }

    ~SstBlock() { delete data_; }

    void Append(Slice *slice) {
        slice->offset_ = write_offset_;
        slice->block_type_ = 1;
        slice->block_id_ = block_id_;
        if (counter_ % 16 == 0 && num_restarts_ < 32) {
            restart_offset_[num_restarts_] = write_offset_;
            num_restarts_++;
        }
        slice->Serialize(data_ + write_offset_);
        write_offset_ += slice->Size();
        counter_++;
    }

    void Read(Slice *slice) {
        size_t offset = slice->offset_;
        slice->Deserialize(data_ + offset);
    }

    bool IsFull() const { return write_offset_ + 128 >= BLOCK_SIZE; }

    void Finshed() {
        std::memcpy(data_, &write_offset_, sizeof(write_offset_));
        std::memcpy(data_ + sizeof(write_offset_), &num_restarts_, sizeof(num_restarts_));
        std::memcpy(data_ + sizeof(write_offset_) + sizeof(num_restarts_), restart_offset_.data(), num_restarts_ * sizeof(uint32_t));
    }

    const int8_t *GetData() const { return data_; }

    void InitFromData(int8_t *data) {
        std::memcpy(&write_offset_, data, sizeof(write_offset_));
        num_restarts_ = *(uint32_t *)(data + sizeof(write_offset_));
        std::memcpy(restart_offset_.data(), data + sizeof(write_offset_) + sizeof(num_restarts_), num_restarts_ * sizeof(uint32_t));
        data_ = std::move(data);
    }

    size_t GetSize() const { return write_offset_ - sizeof(size_t); }

    inline void Serialize(int8_t *buffer) const { std::memcpy(data_, &write_offset_, sizeof(write_offset_)); }

    Iterator *NewIterator(const Comparator *comparator);

private:
    class Iter;

private:
    std::array<uint32_t, 32> restart_offset_;
    size_t write_offset_;
    int8_t *data_;
    uint64_t block_id_;
    uint32_t num_restarts_;
    uint32_t counter_;
};

using SstBlockPtr = std::shared_ptr<SstBlock>;
} // namespace storage
} // namespace rangedb
