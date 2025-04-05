#include "storage/compaction/CompactionTask.h"
#include "storage/block/Block.h"
#include "storage/block/BlockFile.h"
#include "storage/sstblock/Merger.h"
#include "storage/sstblock/SstBlockFile.h"
#include "storage/walblock/WalBlock.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <cstdint>
#include <memory>

using namespace rangedb;

WalBlockFile *CreateWalBlockFileTest() {
    WalBlockFile *block_file = new WalBlockFile(0);
    Slice *slice = new Slice();
    uint64_t block_id = 0;
    for (int i = 0; i < 5 * 1000 * 1000; i++) {
        std::string str_key = "key" + std::to_string(i);
        ByteKey key((int8_t *)str_key.c_str(), str_key.size());
        resp::buffer value((char *)"value", 5);
        slice->block_id_ = 2;
        slice->key_ = key;
        slice->data_ = value;
        slice->version_ = i;
        slice->data_length_ = slice->Size();
        block_file->Append(slice, block_id);
        if (i == (5 * 1000 * 1000 - 1)) {
            std::cout << "GetBlockNum: " << block_file->GetBlockNum() << std::endl;
        }
        if (block_file->GetBlockNum() > 8192) {
            break;
        }
    }
    return block_file;
}

storage::SstBlockFile *CreateSstBlockFileTest() {
    storage::SstBlockFile *block_file = new storage::SstBlockFile(22);
    Slice *slice = new Slice();
    uint64_t block_id = 0;
    for (int i = 5 * 1000 * 1000; i < 10 * 1000 * 1000; i++) {
        std::string str_key = "key" + std::to_string(i);
        ByteKey key((int8_t *)str_key.c_str(), str_key.size());
        resp::buffer value((char *)"value", 5);
        slice->block_id_ = 2;
        slice->key_ = key;
        slice->data_ = value;
        slice->version_ = i;
        slice->data_length_ = slice->Size();
        block_file->Append(slice);
        if (i == (10 * 1000 * 1000 - 1)) {
            std::cout << "GetBlockNum: " << block_file->GetBlockNum() << std::endl;
        }
        if (block_file->GetBlockNum() > 8192) {
            break;
        }
    }
    return block_file;
}

void CompactionTaskTest() {
    storage::BlockFile *wal_block_file = CreateWalBlockFileTest();
    storage::BlockFile *sst_block_file = CreateSstBlockFileTest();
    const Comparator *comparator = ByteKeyComparator();
    CompactionTask *compaction_task = new CompactionTask();
    auto wal_file_ptr = std::shared_ptr<storage::BlockFile>(wal_block_file);
    auto sst_file_ptr = std::shared_ptr<storage::BlockFile>(sst_block_file);
    compaction_task->AddLowLevelFile(wal_file_ptr);
    compaction_task->AddHighLevelFile(sst_file_ptr);
    compaction_task->SetWalFlag(true);
    compaction_task->Compact();
}

TEST(BlockTest, base) { CompactionTaskTest(); }

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}