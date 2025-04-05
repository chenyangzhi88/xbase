#include "storage/sstblock/SstBlock.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include <cstdint>

namespace rangedb {
namespace storage {
class SstBlock::Iter : public Iterator {
private:
    const Comparator *const comparator_;
    const int8_t *const data_;                      // underlying block contents
    const std::array<uint32_t, 32> restart_offset_; // Offset of restart array (list of fixed32)
    uint32_t const num_restarts_;                   // Number of uint32_t entries in restart array
    uint32_t const end_offset_;                     // last key boundary offset
    // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
    uint32_t current_;
    uint32_t restart_index_; // Index of restart block in which current_ falls
    ByteKey key_;
    Slice value_;
    Status status_;

    inline int Compare(const ByteKey &a, const ByteKey &b) const { return comparator_->Compare(a, b); }

    // Return the offset in data_ just past the end of the current entry.
    inline uint32_t NextEntryOffset() const { return current_ + value_.Size(); }

    inline void SeekToRestartPoint(uint32_t index) {
        restart_index_ = index;
        current_ = restart_offset_[index];
        value_.Deserialize((int8_t *)data_ + current_);
        key_ = value_.key_;
    }

public:
    Iter(const Comparator *comparator, const int8_t *data, const std::array<uint32_t, 32> &restart_offset, uint32_t num_restarts,
         uint32_t write_offset)
        : comparator_(comparator), data_(data), restart_offset_(restart_offset), num_restarts_(num_restarts), current_(restart_offset_[0]),
          restart_index_(0), end_offset_(write_offset) {
        assert(num_restarts_ > 0);
    }

    bool Valid() const override { return current_ < end_offset_; }
    Status status() const override { return status_; }

    void Next() override { ParseNextKey(); }

    ByteKey Key() const override {
        // assert(Valid());
        return key_;
    }

    Slice Value() const override {
        // assert(Valid());
        return value_;
    }

    void Prev() override {
        // Scan backwards to a restart point before current_
        const uint32_t original = current_;
        while (restart_offset_[restart_index_] >= original) {
            if (restart_index_ == 0) {
                // No more entries
                current_ = restart_offset_[0];
                restart_index_ = 0;
                return;
            }
            restart_index_--;
        }

        SeekToRestartPoint(restart_index_);
        while (NextEntryOffset() < original) {
            ParseNextKey();
        }
    }

    ByteKey GetRestartPointByteKey(uint32_t region_offset) {
        Slice restart_point;
        restart_point.Deserialize((int8_t *)data_ + restart_offset_[region_offset]);
        return restart_point.key_;
    }

    void Seek(const ByteKey &target) override {
        // Binary search in restart array to find the last restart point
        // with a key < target
        uint32_t left = 0;
        uint32_t right = num_restarts_ - 1;
        int current_key_compare = 0;

        // If we're already scanning, use the current position as a starting
        // point. This is beneficial if the key we're seeking to is ahead of the
        // current position.
        current_key_compare = Compare(key_, target);
        if (current_key_compare < 0) {
            // key_ is smaller than target
            left = restart_index_;
        } else if (current_key_compare > 0) {
            right = restart_index_;
        } else {
            // We're seeking to the key we're already at.
            return;
        }

        while (left < right) {
            uint32_t mid = (left + right + 1) / 2;
            ByteKey mid_key = GetRestartPointByteKey(mid);
            int mid_compare_flag = Compare(mid_key, target) < 0;
            if (mid_compare_flag < 0) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (mid_compare_flag > 0) {
                // Key at "mid" is > "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            } else {
                SeekToRestartPoint(mid);
                return;
            }
        }

        // We might be able to use our current position within the restart block.
        // This is true if we determined the key we desire is in the current block
        // and is after than the current key.
        bool skip_seek = left == restart_index_ && current_key_compare < 0;
        if (!skip_seek) {
            SeekToRestartPoint(left);
        }
        // Linear search (within restart block) for first key >= target
        while (true) {
            if (!ParseNextKey()) {
                return;
            }
            if (Compare(key_, target) >= 0) {
                return;
            }
        }
    }

    void SeekToFirst() override { SeekToRestartPoint(0); }

    void SeekToLast() override {
        SeekToRestartPoint(num_restarts_ - 1);
        while (ParseNextKey()) {
            // Keep skipping
        }
    }

    bool End() const override { return current_ >= end_offset_; }

private:
    bool ParseNextKey() {
        uint32_t next_offset = NextEntryOffset();
        current_ = next_offset;
        if (current_ >= end_offset_) {
            return false;
        }
        value_.Deserialize((int8_t *)data_ + current_);
        key_ = value_.key_;
        return true;
    }
};

Iterator *SstBlock::NewIterator(const Comparator *comparator) {
    return new Iter(comparator, (int8_t *)data_, restart_offset_, num_restarts_, write_offset_);
}

} // namespace storage
} // namespace rangedb