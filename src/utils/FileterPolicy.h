#pragma once

#include "utils/Slice.h"
#include <string>

namespace rangedb {
class Slice;

class FilterPolicy {
public:
    virtual ~FilterPolicy();
    virtual const char *Name() const = 0;
    virtual void CreateFilter(const ByteKey *keys, int n, std::string *dst) const = 0;
    virtual bool KeyMayMatch(const ByteKey &key, const std::string &filter) const = 0;
};
const FilterPolicy *NewBloomFilterPolicy(int bits_per_key);

} // namespace rangdb
