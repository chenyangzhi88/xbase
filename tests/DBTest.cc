#include "server/DB.h"
#include <gtest/gtest.h>
rangedb::DB *db = new rangedb::DB();
void DBTest() {
    rangedb::Slice *slice = new rangedb::Slice();
    // slice->key_ = rangedb::ByteKey((int8_t *)"key", 3);
    // slice->data_ = resp::buffer("value", 5);
    // slice->data_length_ = slice->Size();
    // db->Put(slice);
    slice->data_length_ = 0;
    slice->offset_ = 0;
    rangedb::Slice *get_slice = new rangedb::Slice();
    get_slice->key_ = rangedb::ByteKey((int8_t *)"key", 3);
    db->Get(get_slice);
    get_slice->Print();
    db->FlushWal();
}

void DBInitTest() {
    db->Init();
    DBTest();
}

void TestPutOps(int start, rangedb::DB *db) {
    const int key_size = 5 * 1000 * 1000;
    const auto p1 = std::chrono::system_clock::now();
    for (int i = start; i < key_size + start; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice.offset_ = i;
        slice.version_ = i;
        slice.data_ = resp::buffer(key.c_str(), key.length());
        db->Put(&slice);
    }
    // db->FlushWal();
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "wal put cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    for (int i = start; i < key_size + start; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        db->Get(&slice);
    }
    const auto p3 = std::chrono::system_clock::now();
    std::cout << "wal get cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p3 - p2).count() << "[µs]" << std::endl;
    db->Close();
}

void TestBechmark() {
    int start = 0;
    rangedb::DB *db = new rangedb::DB();
    std::array<std::thread, 40> arr;
    for (int i = 0; i < 6; i++) {
        std::thread t(TestPutOps, i * 1000000, db);
        arr[i] = std::move(t);
    }
    for (int i = 0; i < 6; i++) {
        arr[i].join();
    }
}

TEST(DBTest, base) {
    //rangedb::DB *db = new rangedb::DB();
    // DBInitTest();
    TestPutOps(0, db);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}