// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utils/Comparator.h"

#include "utils/NoDestructor.h"
#include "utils/Slice.h"
namespace rangedb {

Comparator::~Comparator() = default;
namespace {
class ComparatorImpl : public Comparator {
public:
    ComparatorImpl() = default;

    const char *Name() const override { return "rangedb.ByteKeyComparator"; }

    int Compare(const ByteKey &a, const ByteKey &b) const override { return a.Compare(b); }
};
} // namespace

const Comparator *ByteKeyComparator() {
    static NoDestructor<ComparatorImpl> singleton;
    return singleton.get();
}

} // namespace rangedb