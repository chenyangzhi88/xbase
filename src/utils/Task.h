#pragma once

#include "storage/block/BlockFile.h"
#include "utils/Slice.h"
namespace rangedb {

enum TaskType {
    PUT_WAL,
    PUT_INDEX,
    GET_INDEX,
    GET_FROM_CACHE,
    GET_FROM_DISK,
    GET_NEXT_LEVEL,
};

struct Task {
    TaskType action_;
    int level_;
    Slice *slice_;
    coro::event event_;
    volatile bool done_ = false;
    storage::BlockFilePtr block_file_;
    bool flag;
    Task(Slice *slice) : slice_(slice) {}
};
} // namespace rangedb