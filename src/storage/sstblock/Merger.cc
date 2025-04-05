// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "storage/sstblock/Merger.h"

#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"

namespace rangedb {
namespace storage {

class MergingIterator : public Iterator {

public:
    MergingIterator(const Comparator *comparator, std::list<Iterator *> *children) : comparator_(comparator), merge_lst_(children) {}

    ~MergingIterator() {}

    bool Valid() const override { return true; }

    void SeekToFirst() override {
        for (auto iter : *merge_lst_) {
            iter->SeekToFirst();
        }
        FindSmallest();
    }

    void SeekToLast() override {
        for (auto iter : *merge_lst_) {
            iter->SeekToLast();
        }
        FindLargest();
    }

    void Seek(const ByteKey &target) override {
        for (auto iter : *merge_lst_) {
            iter->Seek(target);
        }
        FindSmallest();
    }

    void Next() override {
        assert(Valid());
        iter_->Next();
        if (iter_->End()) {
            merge_lst_->remove(iter_);
        }
        if (!merge_lst_->empty()) {
            FindSmallest();
        }
    }

    void Prev() override {
        assert(Valid());
        FindLargest();
    }

    ByteKey Key() const override {
        assert(Valid());
        return iter_->Key();
    }

    Slice Value() const override {
        assert(Valid());
        return iter_->Value();
    }

    bool End() const override {
        for (auto iter : *merge_lst_) {
            if (!iter->End()) {
                return false;
            }
        }
        return true;
    }

    Status status() const override {
        Status status;
        return status;
    }

private:
    // Which direction is the iterator moving?
    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    const Comparator *comparator_;
    std::list<Iterator *> *merge_lst_;
    Iterator *iter_;
};

void MergingIterator::FindSmallest() {
    Iterator *minest = nullptr;
    for (auto iter : *merge_lst_) {
        if (iter->End()) {
            continue;
        }
        if (minest == nullptr) {
            minest = iter;
        } else if (comparator_->Compare(iter->Key(), minest->Key()) < 0) {
            minest = iter;
        }
    }
    iter_ = minest;
}

void MergingIterator::FindLargest() {
    Iterator *largest = nullptr;
    for (auto iter : *merge_lst_) {
        if (largest == nullptr) {
            largest = iter;
        } else if (comparator_->Compare(iter->Key(), largest->Key()) > 0) {
            largest = iter;
        }
    }
    iter_ = largest;
}

Iterator *NewMergingIterator(const Comparator *comparator, std::list<Iterator *> &iter_lst) {
    int n = iter_lst.size();
    assert(n > 0);
    if (n == 1) {
        return iter_lst.front();
    } else {
        return new MergingIterator(comparator, &iter_lst);
    }
}

} // namespace storage
} // namespace rangedb
