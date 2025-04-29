#pragma once

#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include <atomic>
#include <memory>
namespace rangedb {
namespace storage {
class BlockFile {
public:
    BlockFile() {}
    virtual ~BlockFile() {}
    virtual StatusCode Append(Slice *slice) = 0;
    virtual Status Flush() = 0;
    virtual int GetBlockNum() = 0;
    virtual Iterator *NewIterator(const Comparator *comparator) = 0;
    virtual ByteKey GetMinKey() = 0;
    virtual ByteKey GetMaxKey() = 0;
    virtual Status InitFromData(int8_t *data) = 0;
    virtual Status FoundKey(Slice *key, bool *found) = 0;
    virtual Status MaybeExist(Slice *key, bool *exist) = 0;
    virtual Status Read(size_t inner_block_id, size_t offset, Slice *slice) = 0;
    static std::atomic_uint64_t gloabal_block_id_;
};
using BlockFilePtr = std::shared_ptr<BlockFile>;
} // namespace storage
} // namespace rangedb