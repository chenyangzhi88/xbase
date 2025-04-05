#include "CompactionManager.h"
#include "storage/compaction/CompactionTask.h"
#include "utils/Slice.h"
#include <list>
#include <unistd.h>
namespace rangedb {

void CompactionManager::CreateCompactionTask(int low_level, int high_level) {
    // 1. get low level file
    // 2. get high level file
    // 3. create compaction task
    // 4. add compaction task to compaction task list
    // 5. return
    std::list<storage::BlockFilePtr> low_level_file;
    std::list<storage::BlockFilePtr> high_level_file;
    std::list<storage::BlockFilePtr> to_compact_file;
    ByteKey min_key, max_key;
    std::list<FileInfo *> wal_file_lst;
    file_manager_->GetLevelFile(0, &wal_file_lst);
    std::list<FileInfo *> sst_file_lst;
    file_manager_->GetLevelFile(1, &sst_file_lst);
    for (auto &&file : low_level_file) {
        if (file->GetMinKey() < min_key) {
            min_key = file->GetMinKey();
        }
        if (file->GetMaxKey() > max_key) {
            max_key = file->GetMaxKey();
        }
    }

    for (auto &&file : high_level_file) {
        auto block_file = std::dynamic_pointer_cast<storage::BlockFile>(file);
        if (block_file->GetMaxKey() < min_key) {
            continue;
        }
        if (block_file->GetMinKey() > max_key) {
            continue;
        }
        to_compact_file.emplace_back(file);
    }
    CompactionTask *compaction_task = new CompactionTask();
    for (auto &&file : low_level_file) {
        compaction_task->AddLowLevelFile(file);
    }
    for (auto &&file : to_compact_file) {
        compaction_task->AddHighLevelFile(file);
    }
    if (low_level == 0) {
        compaction_task->SetWalFlag(true);
    }
}

void CompactionManager::ExcuteCompactionTask() {
    for (auto &&compaction_task : compaction_task_list_) {
        compaction_task->Compact();
    }
}

bool CompactionManager::NeedCompaction() {
    int wal_file_num = file_manager_->GetWalFileNum();
    if (wal_file_num > 8) {
        return true;
    }
    return false;
}

void CompactionManager::RunCompactionTask() {
    while (true) {
        // 1. get compaction task
        // 2. compact
        // 3. sleep
        if (NeedCompaction()) {
            CreateCompactionTask(0, 1);
        }
        ExcuteCompactionTask();
        usleep(2000);
    }
}
} // namespace rangedb