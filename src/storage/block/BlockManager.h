#pragma once
#include "storage/block/Block.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/CommonUtil.h"
#include "utils/LruCache.h"
#include <cstdint>
namespace rangedb {
class BlockManager {
private:
    /* data */
    LruCache<std::string, storage::BlockPtr> block_cache_;
    std::unordered_map<std::string, WalBlockFilePtr> wal_block_files_;
    // std::unordered_map<std::string, storage::SstBlockFilePtr> sst_block_files_level_map_;
    std::unordered_map<int, std::vector<std::string>> level_sst_file_map_;
    static std::shared_ptr<BlockManager> block_manager_instance;

public:
    BlockManager(/* args */) : block_cache_(LruCache<std::string, storage::BlockPtr>(1024 * 1024)){};
    ~BlockManager(){};
    bool ReadBlock(Slice *source) {
        uint64_t block_id = source->block_id_;
        uint64_t file_id = source->file_id_;
        std::string key = CommonUtil::Uint128ToString(file_id, block_id);
        if (!block_cache_.exists(key)) {
            return false;
        }
        storage::BlockPtr block = nullptr;
        bool exist = block_cache_.get(key, block);
        block->Read(source);
        return true;
    }

    storage::BlockPtr GetBlock(uint64_t file_id, size_t block_id) {
        storage::BlockPtr block = nullptr;
        std::string key = CommonUtil::Uint128ToString(file_id, block_id);

        if (!block_cache_.exists(key)) {
            return nullptr;
        }
        block_cache_.get(key, block);
        return block;
    }

    bool ReadBlockFromDisk(const std::string &file_name, uint64_t file_id, size_t block_id) {
        storage::BlockPtr block = nullptr;
        if (file_name.find(".wal") != std::string::npos) {
            WalBlockFilePtr block_file = wal_block_files_[file_name]; // get block file by file name
            block = block_file->ReadBlock(block_id);
        }
        AddBlockCache(file_id, block_id, block);
        return true;
    }

    bool WriteBlockToDisk(const std::string &file_name, size_t block_id) { return true; }

    void AddBlockCache(uint64_t file_id, size_t block_id, storage::BlockPtr block) {
        std::string key = CommonUtil::Uint128ToString(file_id, block_id);
        block_cache_.put(key, block);
    }

    static std::shared_ptr<BlockManager> GetInstance();
};
using BlockManagerPtr = std::shared_ptr<BlockManager>;

} // namespace rangedb
