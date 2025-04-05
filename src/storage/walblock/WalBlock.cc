#include "storage/walblock/WalBlock.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"

namespace rangedb {
namespace storage {
class WalBlock::Iter : public Iterator {
private:
    const Comparator *const comparator_;
    const int8_t *const data_; // underlying block contents
    // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
    uint32_t limit_;
    ByteKey key_;
    Slice value_;
    Status status_;
    std::map<ByteKey, uint32_t> key_offset_;
    std::map<ByteKey, uint32_t>::iterator it_;

    inline int Compare(const ByteKey &a, const ByteKey &b) const { return comparator_->Compare(a, b); }

public:
    Iter(const Comparator *comparator, const int8_t *data, uint32_t write_offset)
        : comparator_(comparator), data_(data), limit_(write_offset) {
        uint32_t offset = 16;
        while (offset < limit_) {
            value_.Deserialize((int8_t *)data + offset);
            key_ = value_.key_;
            key_offset_[key_] = offset;
            offset += value_.Size();
        }
    }
    Status status() const override { return status_; }

    bool Valid() const override { return true; }

    void Next() override {
        assert(Valid());
        it_++;
        if (it_ == key_offset_.end()) {
            return;
        }
        ParseKey();
    }
    ByteKey Key() const override {
        assert(Valid());
        return key_;
    }
    Slice Value() const override {
        assert(Valid());
        return value_;
    }
    void Prev() override {
        assert(Valid());
        it_--;
        if (it_ == key_offset_.end()) {
            return;
        }
        ParseKey();
    }

    ByteKey GetRestartPointByteKey(uint32_t region_offset) { return ByteKey(); }

    void Seek(const ByteKey &target) override {
        it_ = key_offset_.lower_bound(target);
        if (it_ == key_offset_.end()) {
            return;
        }
        ParseKey();
    }

    void SeekToFirst() override {
        it_ = key_offset_.begin();
        ParseKey();
    }

    void SeekToLast() override {
        it_ = key_offset_.end();
        it_--;
        ParseKey();
    }

    bool End() const override { return it_ == key_offset_.end(); }

private:
    bool ParseKey() {
        uint32_t offset = it_->second;
        value_.Deserialize((int8_t *)data_ + offset);
        key_ = value_.key_;
        return true;
    }
};

Iterator *WalBlock::NewIterator(const Comparator *comparator) { return new Iter(comparator, (int8_t *)data_, write_offset_); }

} // namespace storage
} // namespace rangedb