#pragma once
#include "coro/coro.hpp"
#include "db/index/HashTable.h"
#include "db/index/MemRangeVector.h"
#include "db/index/RingHashVec.h"
#include "storage/FileManager.h"
#include "storage/block/BlockManager.h"
#include "storage/compaction/CompactionManager.h"
#include "storage/walblock/WalManager.h"
#include "table/LsmTable.h"
#include "utils/Manifest.h"
#include "utils/Status.h"
#include "utils/Task.h"
namespace rangedb {
class DB {
private:
    RingHashVec *mem_vector_;
    FileManager *file_manager_;
    BlockManagerPtr block_manager_;
    WalManagerPtr wal_manager_;
    std::thread wal_put_thread_;
    LsmTable *lsm_table_;
    moodycamel::BlockingConcurrentQueue<Task *> wal_queue_;
    ManifestPtr manifest_ptr_;
    CompactionManager compaction_manager_;
    /* data */
public:
    DB(/* args */);
    ~DB();
    void Apply(Task *task);

    void AppendWal();

    Status Get(Slice *source);

    Status Put(Slice *source);

    void FlushWal();

    void Init();

    void InitIndex();

    void Close();
};
} // namespace rangedb