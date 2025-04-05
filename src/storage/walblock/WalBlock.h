#pragma once

#include "storage/block/Block.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <ostream>
#include <unistd.h>
namespace rangedb {
namespace storage {
struct WalBlockHeader {
    uint32_t write_offset_;
};

class WalBlock : virtual public Block {
public:
    WalBlock(uint64_t block_id) : block_id_(block_id) {
        data_ = new int8_t[BLOCK_SIZE];
        write_offset_ = HEAD_SIZE;
        write_offset_ += sizeof(block_id_);
    }

    ~WalBlock() {
        std::cout << "block_id: " << block_id_ << " already freeed" << std::endl;
        // delete data_;
    }

    void Append(Slice *slice) {
        slice->offset_ = write_offset_;
        slice->block_id_ = block_id_;
        slice->block_type_ = 0;
        slice->data_length_ = slice->Size();
        slice->Serialize(data_ + write_offset_);
        write_offset_ += slice->Size();
        assert(write_offset_ <= BLOCK_SIZE);
    }

    void Append(const uint8_t *data, size_t size) {
        std::memcpy(data_ + write_offset_, data, size);
        write_offset_ += size;
    }

    void Read(Slice *slice) {
        size_t offset = slice->offset_;
        slice->Deserialize(data_ + offset);
    }

    void Finshed() {}

    bool IsFull() const { return write_offset_ + 128 >= BLOCK_SIZE; }

    inline size_t GetSize() const { return write_offset_; }
    inline uint64_t GetBlockId() const { return block_id_; }
    const int8_t *GetData() const { return data_; }
    void InitFromData(int8_t *data) { std::memcpy(&write_offset_, data, sizeof(write_offset_)); }
    void Serialize(int8_t *buffer) const { std::memcpy(data_, &write_offset_, sizeof(write_offset_)); }
    Iterator *NewIterator(const Comparator *comparator);

private:
    class Iter;

private:
    int8_t *data_;
    size_t write_offset_;
    uint64_t block_id_;
    ByteKey max_key_;
    ByteKey min_key_;
};

using WalBlockPtr = std::shared_ptr<WalBlock>;
} // namespace storage
} // namespace rangedb
