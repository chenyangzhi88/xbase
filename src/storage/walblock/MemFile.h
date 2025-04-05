#pragma once

#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include <map>
#include <memory>
namespace rangedb {

class MemFile {
public:
    MemFile(uint64_t block_id) : block_id_(block_id), size_(0) {
        mutable_ = true;
        data_ = std::make_shared<std::map<ByteKey, Slice>>();
    }
    ~MemFile() {}

    void Insert(Slice *slice) {
        data_->insert(std::make_pair(slice->key_, *slice));
        size_ += slice->Size();
    }

    void Read(Slice *slice) {
        auto it = data_->find(slice->key_);
        if (it != data_->end()) {
            *slice = it->second;
        }
    }

    void Finshed() {}

    bool IsFull() const { return false; }

    inline size_t GetSize() const { return size_; }

    void Serialize(int8_t *buffer) const {
        uint32_t offset = 0;
        for (auto &it : *data_.get()) {
            Slice slice;
            slice.key_ = it.first;
            slice = it.second;
            slice.Serialize(buffer + offset);
            offset += slice.Size();
        }
    }

    Iterator *NewIterator(const Comparator *comparator);

private:
    class Iter;

private:
    uint64_t block_id_;
    size_t size_;
    bool mutable_;
    std::shared_ptr<std::map<ByteKey, Slice>> data_;
};
} // namespace rangedb