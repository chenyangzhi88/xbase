#pragma once
#include "storage/FileManager.h"
#include "storage/block/BlockManager.h"
namespace rangedb {
class BlockIOSchedual {
private:
    BlockManagerPtr block_manager_;
    FileManagerPtr file_manager_;
    /* data */
public:
    BlockIOSchedual(/* args */);
    ~BlockIOSchedual();
    void ReadBlock(const std::string &file_name, size_t offset);
};
} // namespace rangedb
