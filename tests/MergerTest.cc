#include "storage/sstblock/Merger.h"
#include "storage/block/Block.h"
#include "storage/sstblock/SstBlockFile.h"
#include "storage/walblock/WalBlock.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <cstdint>

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
    }
    return block_file;
}

storage::SstBlockFile *CreateSstBlockFileTest() {
    storage::SstBlockFile *block_file = new storage::SstBlockFile(0);
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
        if (block_file->GetBlockNum() >= 8192) {
            break;
        }
    }
    return block_file;
}

void IterateTest() {
    WalBlockFile *wal_block_file = CreateWalBlockFileTest();
    storage::SstBlockFile *sst_block_file = CreateSstBlockFileTest();
    const Comparator *comparator = ByteKeyComparator();
    Iterator *wal_iter = wal_block_file->NewIterator(comparator);
    std::list<Iterator *> file_lst;
    file_lst.push_back(wal_iter);
    Iterator *sst_iter = sst_block_file->NewIterator(comparator);
    file_lst.push_back(sst_iter);
    Iterator *iter = storage::NewMergingIterator(comparator, file_lst);
    iter->SeekToFirst();
    while (!iter->End()) {
        ByteKey key = iter->Key();
        Slice value = iter->Value();
        std::cout << "key: " << key.ToString() << std::endl;
        iter->Next();
    }
}

TEST(BlockTest, base) { IterateTest(); }

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}