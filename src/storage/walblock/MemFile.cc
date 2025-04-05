#include "storage/walblock/MemFile.h"
#include "utils/Iterator.h"
#include <memory>

namespace rangedb {
class MemFile::Iter : public Iterator {
private:
    struct cmp {
        const Comparator *comparator_;
        bool operator()(const Iterator *a, const Iterator *b) const { return b->Key() < a->Key(); }
    };
    const Comparator *const comparator_;
    std::shared_ptr<std::map<ByteKey, Slice>> data_;
    std::map<ByteKey, Slice>::iterator iter_;
    Slice value_;
    ByteKey key_;
    bool asc;
    Status status_;

public:
    Iter(const Comparator *comparator, const std::shared_ptr<std::map<ByteKey, Slice>> &data) : comparator_(comparator), asc(true) {
        data_ = data;
    }

    bool Valid() const override { return true; }
    Status status() const override { return status_; }

    void Next() override {
        // assert(Valid());
        if (iter_ == data_->end()) {
            return;
        }
        key_ = (*iter_).first;
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
        if (iter_ == data_->end()) {
            return true;
        }
        return false;
    }
    void Prev() override { assert(Valid()); }

    ByteKey GetRestartPointByteKey(uint32_t region_offset) { return ByteKey(); }

    void Seek(const ByteKey &target) override {}

    void SeekToFirst() override {}

    void SeekToLast() override {}
};

Iterator *MemFile::NewIterator(const Comparator *comparator) {
    std::shared_ptr<std::map<ByteKey, Slice>> data;
    if (mutable_) {
        for (auto &it : *data_.get()) {
            data_->insert(std::make_pair(it.first, it.second));
        }
    }
    return new Iter(comparator, data);
}
} // namespace rangedb