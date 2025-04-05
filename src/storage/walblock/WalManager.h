#pragma once
#include "storage/FileManager.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Manifest.h"
#include "utils/Status.h"
#include <list>
#include <thread>

namespace rangedb {
class WalManager {
public:
    using Ptr = std::shared_ptr<WalManager>;

    WalManager();

    Status CreateBlockFile(size_t file_id);

    StatusCode Put(Slice *source);

    StatusCode Get(Slice *source);

    StatusCode Flush();

    inline void NextBlockId() { db_block_id_++; }

private:
    WalBlockFilePtr current_block_file_;
    std::list<WalBlockFilePtr> to_flush_list_;
    std::thread flush_thread_;
    FileManager* file_manager_;
    std::mutex mutex_;
    uint64_t db_file_id_ = 0;
    FileInfo *current_file_info_ = nullptr;
    std::uint64_t db_block_id_ = 0;

}; // WalManager
using WalManagerPtr = std::shared_ptr<WalManager>;
} // namespace rangedb