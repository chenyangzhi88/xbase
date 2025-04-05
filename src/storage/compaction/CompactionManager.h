
#pragma once
#include "storage/FileManager.h"
#include "storage/compaction/CompactionTask.h"
#include <sys/types.h>
#include <thread>
namespace rangedb {
class CompactionManager {
public:
    CompactionManager() {}
    ~CompactionManager() {}
    void CreateCompactionTask(int low_level, int high_level);
    void ExcuteCompactionTask();
    void RunCompactionTask();
    bool NeedCompaction();

private:
    std::thread compaction_thread_;
    std::list<CompactionTaskPtr> compaction_task_list_;
    FileManagerPtr file_manager_;
};
} // namespace rangedb