
#pragma once

#include "storage/FileManager.h"
#include "storage/block/Block.h"
#include "storage/block/BlockManager.h"
#include "utils/FileHandle.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include <cstdint>
#include <list>
#include <memory>
#include <unistd.h>

namespace rangedb {
namespace storage {

class SstBlockFile : virtual public BlockFile {
public:
    SstBlockFile(uint64_t file_id) {
        file_id_ = file_id;
        file_name_ = std::to_string(file_id) + ".sst";
        block_manager_ = BlockManager::GetInstance();
        file_handle_ = FileManager::GetInstance()->CreateFile(file_name_);
    }
    ~SstBlockFile() {}
    storage::BlockPtr AddBlock();

    // Read data from file
    BlockPtr ReadBlock(size_t inner_block_id);

    inline int GetBlockNum() override { return block_num_; }

    StatusCode Append(Slice *source) override;

    Status Flush() override;

    Status InitFromData(int8_t *data) override;

    Iterator *NewIterator(const Comparator *comparator) override;

    ByteKey GetMinKey() override { return file_min_key_; }

    ByteKey GetMaxKey() override { return file_max_key_; }

    void SetKey(const ByteKey& min_key, const ByteKey& max_key) {
        file_min_key_ = min_key;
        file_max_key_ = max_key;
    }

private:
    class Iter;

private:
    uint64_t file_id_;
    std::string file_name_;
    std::list<storage::BlockPtr> block_list_;
    uint32_t block_num_ = 0;
    uint64_t cur_append_block_id_ = 0;
    FileHandlePtr file_handle_;
    BlockManagerPtr block_manager_;
    ByteKey file_max_key_;
    ByteKey file_min_key_;
    std::vector<ByteKey> block_min_key_vec_;
};
using SstBlockFilePtr = std::shared_ptr<SstBlockFile>;
} // namespace storage
} // namespace rangedb