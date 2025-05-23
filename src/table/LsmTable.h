#pragma once

#include "db/index/RingHashVec.h"
#include "storage/FileManager.h"
#include "storage/walblock/WalBlock.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include "utils/Task.h"
namespace rangedb {
class LsmTable {

public:
    LsmTable(RingHashVec *mem_vector_);
    ~LsmTable(){};
    Status GetFromMemBlock(Slice *source);
    Status Put(Slice *source);
    Status GetFromLevelFile(Slice *source, Task *task);
    void BuildSstFile();

private:
    std::atomic<uint64_t> db_file_id_;
    WalBlockFile *mutable_mem_block_;
    RingHashVec *mem_vector_;
    std::list<WalBlockFile*> unmutabl_mem_file_list_;
    std::thread build_thread_;
    FileManager *file_manager_;
};
} // namespace rangedb