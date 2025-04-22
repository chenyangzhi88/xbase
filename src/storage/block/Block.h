#pragma once

#include <cstdint>
#include <cstring>
#include <unistd.h>

#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include <memory>

namespace rangedb {
namespace storage {

const size_t BLOCK_SIZE = 64 * 1024;
const size_t SST_BLOCKFILE_HEADER_SIZE = 1024 * 1024;
const size_t HEAD_SIZE = 8;
const size_t MAX_BLOCK_NUM = 4 * 1024;

struct BlockHeader {
    uint32_t offset_;
    /* data */
};

class Block {
public:
    Block(){};

    virtual ~Block() {}

    virtual void Append(Slice *slice) = 0;

    virtual void Read(Slice *slice) = 0;

    virtual bool IsFull() const = 0;
    virtual size_t GetSize() const = 0;
    virtual void Finshed() = 0;
    virtual const int8_t *GetData() const = 0;
    virtual void InitFromData(int8_t *data) = 0;
    virtual Iterator *NewIterator(const Comparator *comparator) = 0;
};

using BlockPtr = std::shared_ptr<Block>;
} // namespace storage
} // namespace rangedb
