#include "storage/block/BlockIOSchedual.h"
#include "storage/block/Block.h"

namespace rangedb {
BlockIOSchedual::BlockIOSchedual(/* args */) {}
void BlockIOSchedual::ReadBlock(const std::string &filename, size_t block_id) {
    storage::BlockPtr block = nullptr;
    if (filename.find(".wal") != std::string::npos) {
        // WalBlockFile block_file(filename.c_str());
        // block_manager_->ReadBlock(filename, block_id);
        // uint64_t inner_block_id = block_id - std::stoi(filename.substr(0, filename.find(".")));
        // block = block_file->ReadBlock(inner_block_id);
    }
    // block_manager_->AddBlockCache(block_id, block);
    //  Read block from disk
}
} // namespace rangedb