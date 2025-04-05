#include "storage/walblock/WalBlockFile.h"
#include "storage/block/Block.h"
#include "storage/walblock/WalBlock.h"
#include "utils/Comparator.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <cstdint>
#include <string>

using namespace rangedb;

WalBlockFile *CreateBlockFileTest() {
    WalBlockFile *block_file = new WalBlockFile(0);
    Slice *slice = new Slice();
    uint64_t block_id = 0;
    for (int i = 0; i < 50000; i++) {
        std::string str_key = std::to_string(i);
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

void TestBlockList() {
    std::list<storage::BlockPtr> block_list;
    for (int i = 0; i < 60000; i++) {
        auto block = std::make_shared<storage::WalBlock>(i);
        block_list.emplace_back(block);
    }
    block_list.clear();
}

void PutAndGetTest() {
    WalBlockFile *block_file = new WalBlockFile(0);
    Slice *slice_0 = new Slice();
    uint64_t block_id = 0;
    ByteKey key((int8_t *)"key", 3);
    resp::buffer value((char *)"value", 5);
    slice_0->block_id_ = 2;
    slice_0->key_ = key;
    slice_0->data_ = value;
    slice_0->version_ = 1;
    slice_0->data_length_ = slice_0->Size();
    block_file->Append(slice_0, block_id);

    Slice *slice_1 = new Slice();
    ByteKey key_1((int8_t *)"key1", 4);
    resp::buffer value_1((char *)"value1", 6);
    slice_1->block_id_ = 2;
    slice_1->key_ = key_1;
    slice_1->data_ = value_1;
    slice_1->version_ = 1;
    slice_1->data_length_ = slice_1->Size();
    block_file->Append(slice_1, block_id);
}

void IterateTest(WalBlockFile *block_file) {
    const Comparator *comparator = ByteKeyComparator();
    Iterator *iter = block_file->NewIterator(comparator);
    iter->SeekToFirst();
    while (!iter->End()) {
        ByteKey key = iter->Key();
        Slice value = iter->Value();
        std::cout << "key: " << key.ToString() << std::endl;
        iter->Next();
    }
    ByteKey key_1((int8_t *)"keyaa", 5);
    iter->Seek(key_1);
    while (!iter->End()) {
        ByteKey key = iter->Key();
        Slice value = iter->Value();
        std::cout << "key: " << key.ToString() << std::endl;
        iter->Next();
    }
}

TEST(BlockTest, base) {
    auto block_file = CreateBlockFileTest();
    IterateTest(block_file);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}