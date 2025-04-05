#include "table/LsmTable.h"

#include "db/index/RingHashVec.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"

TEST(MemFileTest, mem_file_test) {
    rangedb::RingHashVec *mem_vector = new rangedb::RingHashVec();
    rangedb::LsmTable lsm_table(mem_vector);
    rangedb::Slice slice;
    slice.key_ = rangedb::ByteKey((int8_t *)"key", 3);
    slice.offset_ = 1;
    slice.version_ = 1;
    lsm_table.Put(&slice);
    rangedb::Slice slice1;
    slice1.offset_ = slice.offset_;
    slice1.block_id_ = slice.block_id_;

    slice1.key_ = rangedb::ByteKey((int8_t *)"key", 3);
    lsm_table.GetFromMemBlock(&slice1);
    slice1.Print();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}