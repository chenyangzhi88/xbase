#include "db/index/HashTable.h"
#include "utils/Slice.h"
#include "xxhash.h"
#include "gtest/gtest.h"
#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <sys/mman.h>

void test6() {
    // MaskEmptyTest();
    const auto p11 = std::chrono::system_clock::now();
    // test3();
    const auto p12 = std::chrono::system_clock::now();
    std::cout << "sha256 cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p12 - p11).count() << "[µs]" << std::endl;
    rangedb::db::HashTable *hash_table_test = new rangedb::db::HashTable();
    std::vector<std::string> keys;
    const auto p1 = std::chrono::system_clock::now();
    const int key_size = 1000;
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice slice;
        std::string key = std::to_string(i);
        keys.push_back(key); // 将字符串存储在 vector 中
        slice.key_ = rangedb::ByteKey((int8_t *)keys.back().c_str(), keys.back().length());
        slice.offset_ = i;
        slice.version_ = i;
        hash_table_test->put(&slice);
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "hash insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice slice;
        slice.key_ = rangedb::ByteKey((int8_t *)keys[i].c_str(), keys[i].length());
        slice.offset_ = i;
        slice.version_ = i;
        bool result = hash_table_test->get(&slice);
        if (!result) {
            // std::cout << "key: " << std::to_string(i) << " value: " << slice.value_ << std::endl;
            std::cout << "get key: " << std::to_string(i) << " not found" << std::endl;
        }
    }
    const auto p_get = std::chrono::system_clock::now();
    std::cout << "hash get time = " << std::chrono::duration_cast<std::chrono::microseconds>(p_get - p2).count() << "[µs]" << std::endl;
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice slice;
        slice.key_ = rangedb::ByteKey((int8_t *)keys[i].c_str(), keys[i].length());
        slice.offset_ = i;
        bool result = hash_table_test->del(&slice);
        if (!result) {
            std::cout << "delete key: " << std::to_string(i) << " not found" << std::endl;
        }
        /*
        for (int j = i + 1; j < key_size; j++) {
            rangedb::Slice slice;
            slice.key_ = rangedb::ByteKey((int8_t*)keys[j].c_str(), keys[j].length());
            slice.offset_ = j;
            slice.version_ = j;
            bool result = hash_table_test->get(&slice);
            if (!result) {
                std::cout << "get key: " << std::to_string(j) << " not found" << std::endl;
            }
        }
        */
        // hash_table_test->Print();
    }
    const auto p3 = std::chrono::system_clock::now();
    // hash_table_test->Print();
    std::cout << "hash delete time = " << std::chrono::duration_cast<std::chrono::microseconds>(p3 - p2).count() << "[µs]" << std::endl;
}

void TestSplit() {
    rangedb::db::HashTable *hash_table_test = new rangedb::db::HashTable();
    std::vector<std::string> keys;
    const int key_size = 1000;
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice slice;
        std::string key = std::to_string(i);
        keys.push_back(key); // 将字符串存储在 vector 中
        slice.key_ = rangedb::ByteKey((int8_t *)keys.back().c_str(), keys.back().length());
        slice.offset_ = i;
        slice.version_ = i;
        hash_table_test->put(&slice);
    }

    std::pair<int, std::vector<rangedb::Slice>> pair = hash_table_test->Split();
    std::cout << "mid index: " << pair.second[pair.first].key_.ToString() << std::endl;
    for (int i = 0; i < pair.second.size(); i++) {
        if (i < pair.first) {
            ASSERT_TRUE(pair.second[i].key_ < pair.second[pair.first].key_);
        } else if (i > pair.first) {
            ASSERT_TRUE(pair.second[i].key_ > pair.second[pair.first].key_);
        }
    }
}

TEST(HashTableTest, base) {
    test6();
    // TestSplit();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}