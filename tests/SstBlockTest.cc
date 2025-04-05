#include "storage/sstblock/SstBlock.h"
#include "utils/Comparator.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <string>

using namespace rangedb;

storage::SstBlock *CreateBlockTest() {
    storage::SstBlock *block = new storage::SstBlock(0);
    Slice *slice = new Slice();
    for (int i = 0; i < 500; i++) {
        std::string str_key = "key" + std::to_string(i);
        ByteKey key((int8_t *)str_key.c_str(), str_key.size());
        resp::buffer value((char *)"value", 5);
        slice->block_id_ = 2;
        slice->key_ = key;
        slice->data_ = value;
        slice->version_ = i;
        slice->data_length_ = slice->Size();
        block->Append(slice);
    }
    return block;
}

storage::SstBlock *PutAndGetTest() {
    storage::SstBlock *block = new storage::SstBlock(0);
    Slice *slice_0 = new Slice();
    ByteKey key((int8_t *)"key", 3);
    resp::buffer value((char *)"value", 5);
    slice_0->block_id_ = 2;
    slice_0->key_ = key;
    slice_0->data_ = value;
    slice_0->version_ = 1;
    slice_0->data_length_ = slice_0->Size();
    block->Append(slice_0);

    Slice *slice_1 = new Slice();
    ByteKey key_1((int8_t *)"key1", 4);
    resp::buffer value_1((char *)"value1", 6);
    slice_1->block_id_ = 2;
    slice_1->key_ = key_1;
    slice_1->data_ = value_1;
    slice_1->version_ = 1;
    slice_1->data_length_ = slice_1->Size();
    block->Append(slice_1);

    Slice *slice_2 = new Slice();
    slice_2->block_id_ = 2;
    slice_2->offset_ = slice_0->offset_;
    block->Read(slice_2);
    slice_2->Print();

    Slice *slice_3 = new Slice();
    slice_3->block_id_ = 2;
    slice_3->offset_ = slice_1->offset_;
    block->Read(slice_3);
    slice_3->Print();
    return block;
}

void IterateTest(storage::SstBlock *block) {
    const Comparator *comparator = ByteKeyComparator();
    Iterator *iter = block->NewIterator(comparator);
    iter->SeekToFirst();
    while (!iter->End()) {
        ByteKey key = iter->Key();
        Slice value = iter->Value();
        std::cout << "key: " << key.ToString() << std::endl;
        iter->Next();
    }
    ByteKey key_1((int8_t *)"keya", 4);
    iter->Seek(key_1);
    while (!iter->End()) {
        ByteKey key = iter->Key();
        Slice value = iter->Value();
        std::cout << "key: " << key.ToString() << std::endl;
        iter->Next();
    }
}

TEST(BlockTest, base) {
    auto block = CreateBlockTest();
    IterateTest(block);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}