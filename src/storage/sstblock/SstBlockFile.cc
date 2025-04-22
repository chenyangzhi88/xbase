#include "storage/sstblock/SstBlockFile.h"
#include "storage/block/Block.h"
#include "storage/sstblock/SstBlock.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include <atomic>
#include <cstdint>
#include <vector>

namespace rangedb {
namespace storage {

std::atomic_uint64_t BlockFile::gloabal_block_id_ = 0;

storage::BlockPtr SstBlockFile::AddBlock() {
    //assert(block_num_ < storage::MAX_BLOCK_NUM);
    auto new_block = std::make_shared<storage::SstBlock>(block_num_);
    block_list_.emplace_back(new_block);
    BlockManager::GetInstance()->AddBlockCache(file_id_, block_num_, new_block);
    block_num_++;
    return new_block;
}

BlockPtr SstBlockFile::ReadBlock(size_t inner_block_id) {
    BlockPtr block = std::make_shared<storage::SstBlock>(inner_block_id);
    int8_t *data = new int8_t[BLOCK_SIZE];
    block->InitFromData(data);
    file_handle_->Read(data, BLOCK_SIZE, inner_block_id * BLOCK_SIZE + SST_BLOCKFILE_HEADER_SIZE);
    return block;
}

StatusCode SstBlockFile::Append(Slice *source) {
    if (block_list_.empty()) {
        AddBlock();
    }
    auto block = block_list_.back();

    //if (storage::BLOCK_SIZE - block->GetSize() < source->Size() && block_num_ == storage::MAX_BLOCK_NUM) {
    //    return SST_BLOCK_FULL_ERROR;
    //}

    if (storage::BLOCK_SIZE - block->GetSize() < source->Size()) {
        block_min_key_vec_.emplace_back(source->key_);
        block = AddBlock();
    }
    block->Append(source);
    return DB_SUCCESS;
}

Status SstBlockFile::Flush() {
    // build blockfile header
    int8_t *header = new int8_t[storage::SST_BLOCKFILE_HEADER_SIZE];
    uint32_t offset = 0;
    std::memcpy(header + offset, &block_num_, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    for (int i = 0; i < block_min_key_vec_.size(); i++) {
        std::memcpy(header + offset, &block_min_key_vec_[i].length_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(header + offset, block_min_key_vec_[i].data_, block_min_key_vec_[i].length_);
        offset += block_min_key_vec_[i].length_;
    }
    // Flush header to file
    file_handle_->Write(header, storage::SST_BLOCKFILE_HEADER_SIZE);
    // Flush data to file
    for (auto &&iter = block_list_.begin(); iter != block_list_.end(); iter++) {
        auto block = *iter;
        block->Finshed();
        file_handle_->Write(block->GetData(), storage::BLOCK_SIZE);
    }
    file_handle_->Sync();
    auto last_block = block_list_.back();
    block_list_.clear();
    block_list_.emplace_back(last_block);
    return Status::OK();
}

Status SstBlockFile::InitFromData(int8_t *data) {
    uint32_t offset = 0;
    std::memcpy(&block_num_, data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    for (int i = 0; i < block_num_ - 1; i++) {
        uint32_t length_ = 0;
        std::memcpy(&length_, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        ByteKey key((int8_t *)data + offset, length_);
        block_min_key_vec_.emplace_back(key);
        offset += length_;
    }
    offset = storage::SST_BLOCKFILE_HEADER_SIZE;
    for (int i = 0; i < block_num_; i++) {
        auto block = std::make_shared<storage::SstBlock>(i);
        block->InitFromData(data + offset);
        block_list_.emplace_back(block);
        block_manager_->AddBlockCache(file_id_, i, block);
        offset += storage::BLOCK_SIZE;
    }
    return Status::OK();
}

class SstBlockFile::Iter : public Iterator {
private:
    const Comparator *const comparator_;
    uint64_t file_id_;
    uint64_t start_block_id_;
    uint64_t current_block_id_;
    BlockPtr block_;
    Iterator *iter_;
    uint32_t num_restarts_;
    BlockManagerPtr block_manager_;
    const std::vector<ByteKey> *restart_offset_;
    Status status_;
    inline int Compare(const ByteKey &a, const ByteKey &b) const { return comparator_->Compare(a, b); }

    // Return the offset in data_ just past the end of the current entry.
    inline uint32_t NextEntryOffset() const {

        return 0;
        // return current_ + value_.Size();
    }

    void SeekToRestartPoint(uint32_t index) {
        block_ = block_manager_->GetBlock(file_id_, index);
        iter_ = block_->NewIterator(comparator_);
        // ParseNextKey() starts at the end of value_, so set value_ accordingly
        // uint32_t offset = GetRestartPoint(index);
    }

public:
    Iter(const Comparator *comparator, std::vector<ByteKey> *restart_offset, uint64_t file_id)
        : comparator_(comparator), restart_offset_(restart_offset), start_block_id_(0), current_block_id_(start_block_id_),
          num_restarts_(restart_offset->size()), file_id_(file_id) {
        assert(num_restarts_ >= 0);
        block_manager_ = BlockManager::GetInstance();
    }

    bool Valid() const override { return true; }
    Status status() const override { return status_; }

    void Next() override {
        assert(Valid());
        iter_->Next();
        //std::cout << "block_id: " << current_block_id_ << std::endl;
        if (iter_->End() && current_block_id_ < num_restarts_) {
            current_block_id_++;
            block_ = block_manager_->GetBlock(file_id_, current_block_id_);
            iter_ = block_->NewIterator(comparator_);
            iter_->SeekToFirst();
        }
    }
    ByteKey Key() const override {
        // assert(Valid());
        return iter_->Key();
    }

    Slice Value() const override {
        assert(Valid());
        return iter_->Value();
    }

    bool End() const override {
        if (current_block_id_ == num_restarts_ && iter_->End()) {
            return true;
        }
        return false;
    }
    void Prev() override {
        assert(Valid());
        if (iter_->End()) {
            block_ = block_manager_->GetBlock(file_id_, current_block_id_ - 1);
        } else {
            iter_->Prev();
        }
        // Scan backwards to a restart point before current_
        const uint32_t original = current_block_id_;
        uint32_t restart_index_ = current_block_id_ - start_block_id_;
        while (iter_->End()) {
            if (current_block_id_ == 0) {
                // No more entries
                current_block_id_ = start_block_id_;
                restart_index_ = 0;
                return;
            }
            restart_index_--;
        }

        SeekToRestartPoint(restart_index_);
        do {
            // Loop until end of current entry hits the start of original entry
        } while (ParseNextKey() && NextEntryOffset() < original);
    }

    ByteKey GetRestartPointByteKey(uint32_t region_offset) { return ByteKey(); }

    void Seek(const ByteKey &target) override {
        // Binary search in restart array to find the last restart point
        // with a key < target
        uint32_t left = 0;
        uint32_t right = num_restarts_ - 1;
        int current_key_compare = 0;
        uint32_t target_offset = 0;
        while (left < right) {
            uint32_t mid = (left + right + 1) / 2;
            ByteKey mid_key = restart_offset_->at(mid);
            int compare_flag = Compare(mid_key, target);
            if (compare_flag < 0) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (compare_flag > 0) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            } else {
                target_offset = mid;
                break;
            }
        }
        target_offset = left;
        // We might be able to use our current position within the restart block.
        // This is true if we determined the key we desire is in the current block
        // and is after than the current key.
        block_ = block_manager_->GetBlock(file_id_, current_block_id_ - 1);
        iter_ = block_->NewIterator(comparator_);
        iter_->Seek(target);
    }

    void SeekToFirst() override {
        SeekToRestartPoint(0);
        iter_->SeekToFirst();
    }

    void SeekToLast() override {
        SeekToRestartPoint(num_restarts_);
        iter_->SeekToLast();
    }

private:
    bool ParseNextKey() {
        iter_->Next();
        return true;
    }
};

Iterator *SstBlockFile::NewIterator(const Comparator *comparator) { return new Iter(comparator, &block_min_key_vec_, file_id_); }

} // namespace storage
} // namespace rangedb
