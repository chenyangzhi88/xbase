#include "storage/walblock/MemFile.h"

#include "utils/Slice.h"
#include "gtest/gtest.h"

TEST(MemFileTest, mem_file_test) {
    rangedb::MemFile mem_file(1);
    rangedb::Slice slice;
    slice.key_ = rangedb::ByteKey((int8_t *)"key", 3);
    slice.offset_ = 1;
    slice.version_ = 1;
    mem_file.Insert(&slice);
    rangedb::Slice slice1;
    slice1.key_ = rangedb::ByteKey((int8_t *)"key", 3);
    mem_file.Read(&slice1);
    slice1.Print();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}