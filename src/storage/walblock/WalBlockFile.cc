// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <queue>
#include <vector>

#include "storage/block/Block.h"
#include "storage/block/BlockManager.h"
#include "storage/walblock/WalBlock.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Comparator.h"
#include "utils/Error.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"

namespace rangedb {
WalBlockFile::WalBlockFile(uint64_t file_id) : file_id_(file_id) {}

storage::BlockPtr WalBlockFile::AddBlock() {
    assert(block_num_ < storage::MAX_BLOCK_NUM);
    auto new_block = std::make_shared<storage::WalBlock>(block_num_);
    block_list_.emplace_back(new_block);
    block_num_++;
    return new_block;
}

StatusCode WalBlockFile::Append(Slice *source, uint32_t db_block_id) {
    if (block_list_.empty()) {
        AddBlock();
    }
    auto block = block_list_.back();
    if (storage::BLOCK_SIZE - block->GetSize() < source->Size()) {
        block = AddBlock();
    }
    block->Append(source);
    return DB_SUCCESS;
}

StatusCode WalBlockFile::Append(Slice *source) {
    if (block_list_.empty()) {
        AddBlock();
    }
    auto block = block_list_.back();
    if (storage::BLOCK_SIZE - block->GetSize() < source->Size()) {
        block = AddBlock();
    }
    block->Append(source);
    return DB_SUCCESS;
}

size_t WalBlockFile::remind() {
    size_t remind_size = storage::BLOCK_SIZE;
    if (!block_list_.empty()) {
        remind_size = storage::BLOCK_SIZE - block_list_.back()->GetSize() + (storage::MAX_BLOCK_NUM - block_num_) * storage::BLOCK_SIZE;
    }
    return remind_size;
}

storage::BlockPtr WalBlockFile::ReadBlock(uint32_t block_id) {
    uint32_t i = 0;
    storage::BlockPtr block = block_list_[block_id];
    assert(block != nullptr);
    return block;
}

Status WalBlockFile::Flush() {
    Status status;
    return status;
}

Status WalBlockFile::InitFromData(int8_t *data) {
    uint32_t offset = 0;
    std::memcpy(&block_num_, data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    for (int i = 0; i < block_num_; i++) {
        auto block = std::make_shared<storage::WalBlock>(i);
        block->InitFromData(data + offset);
        block_list_.emplace_back(block);
        BlockManager::GetInstance()->AddBlockCache(file_id_, i, block);
        offset += storage::BLOCK_SIZE;
    }
    return Status::OK();
}

class WalBlockFile::Iter : public Iterator {
private:
    struct cmp {
        const Comparator *comparator_;
        bool operator()(const Iterator *a, const Iterator *b) const { return b->Key() < a->Key(); }
    };
    const Comparator *const comparator_;
    uint64_t start_block_id_;
    uint64_t current_block_id_;
    std::vector<storage::BlockPtr> wal_block_list_;
    std::priority_queue<Iterator *, std::vector<Iterator *>, cmp> iter_queue_;
    Iterator *iter_;
    BlockManagerPtr block_manager_;
    Slice value_;
    ByteKey key_;
    bool asc;
    Status status_;
    // Return the offset in data_ just past the end of the current entry.
    inline uint32_t NextEntryOffset() const {
        return 0;
        // return current_ + value_.Size();
    }

public:
    Iter(const Comparator *comparator, const std::vector<storage::BlockPtr> &wal_block_list)
        : comparator_(comparator), start_block_id_(0), current_block_id_(start_block_id_), iter_queue_(cmp(comparator)), asc(true) {
        assert(wal_block_list.size() >= 0);
        wal_block_list_ = wal_block_list;
    }

    bool Valid() const override { return current_block_id_ < 0; }
    Status status() const override { return status_; }

    void Next() override {
        // assert(Valid());
        if (iter_queue_.empty()) {
            return;
        }
        iter_ = iter_queue_.top();
        key_ = iter_->Key();
        value_ = iter_->Value();
        iter_queue_.pop();
        if (!iter_->End()) {
            iter_->Next();
            iter_queue_.push(iter_);
        }
    }
    ByteKey Key() const override {
        // assert(Valid());
        return key_;
    }

    Slice Value() const override {
        // assert(Valid());
        return value_;
    }

    bool End() const override {
        if (iter_queue_.empty()) {
            return true;
        }
        return false;
    }
    void Prev() override { assert(Valid()); }

    ByteKey GetRestartPointByteKey(uint32_t region_offset) { return ByteKey(); }

    void Seek(const ByteKey &target) override {
        for (auto &&block : wal_block_list_) {
            iter_ = block->NewIterator(comparator_);
            iter_->Seek(target);
            if (iter_->End()) {
                continue;
            }
            iter_queue_.push(iter_);
        }
        key_ = iter_queue_.top()->Key();
        value_ = iter_queue_.top()->Value();
    }

    void SeekToFirst() override {
        for (auto &&block : wal_block_list_) {
            iter_ = block->NewIterator(comparator_);
            iter_->SeekToFirst();
            iter_queue_.push(iter_);
        }
        iter_ = iter_queue_.top();
        key_ = iter_->Key();
        value_ = iter_->Value();
        iter_queue_.pop();
        if (!iter_->End()) {
            iter_->Next();
            iter_queue_.push(iter_);
        }
    }

    void SeekToLast() override {
        for (auto &&block : wal_block_list_) {
            iter_ = block->NewIterator(comparator_);
            iter_->SeekToLast();
            iter_queue_.push(iter_);
        }
    }

private:
    bool ParseNextKey() {
        iter_->Next();
        return true;
    }
};

Iterator *WalBlockFile::NewIterator(const Comparator *comparator) { return new Iter(comparator, block_list_); }

} // namespace rangedb
