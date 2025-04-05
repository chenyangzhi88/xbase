#include "storage/walblock/WalManager.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"

void WalManagerTest() {
    rangedb::WalManager wal_manager;
    rangedb::Slice *slice = new rangedb::Slice();
    slice->key_ = rangedb::ByteKey((int8_t *)"key", 3);
    slice->data_length_ = slice->Size();
    slice->version_ = 123;
    wal_manager.Put(slice);
    wal_manager.Flush();
    slice->version_ = 0;
    slice->key_.hash_0_ = 0;
    slice->block_id_ = 0;
    // rangedb::BlockManagerPtr block_manager = rangedb::BlockManager::GetInstance();
    rangedb::storage::BlockPtr block = nullptr;
    slice->version_ = 0;
    // block_manager->ReadBlock(slice);
    EXPECT_EQ(slice->version_, 123);
}

TEST(RangeSkiplistTest, base) { WalManagerTest(); }

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}
