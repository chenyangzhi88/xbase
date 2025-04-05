#pragma once
#include "storage/FileManager.h"
#include "storage/block/BlockFile.h"
#include "utils/Comparator.h"
#include <fcntl.h>
#include <list>
namespace rangedb {
class MergingIterator;
class CompactionTask {
public:
    CompactionTask() {
        cmp_ = ByteKeyComparator();
        file_manager_ = FileManager::GetInstance();
    }
    ~CompactionTask() {}
    void AddLowLevelFile(storage::BlockFilePtr file);
    void AddHighLevelFile(storage::BlockFilePtr file);
    void SetWalFlag(bool flag) { wal_compact_flag_ = flag; }
    void Compact();
    void WalBlockToCompaction();
    void SstBlockToCompaction();

private:
    /* data */
    std::list<storage::BlockFilePtr> low_level_file_;
    std::list<storage::BlockFilePtr> high_level_file_;
    bool wal_compact_flag_;
    const Comparator *cmp_;
    FileManager *file_manager_;
};
using CompactionTaskPtr = std::shared_ptr<CompactionTask>;
} // namespace rangedb
