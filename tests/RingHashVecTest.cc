#include "db/index/RingHashVec.h"
#include "coro/coro.hpp"
#include "db/index/RangeSkiplist.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <iostream>
#include <thread>
#include <unordered_map>

void TestCoroTask() {
    // Task that takes a value and doubles it.
    std::vector<coro::task<uint64_t>> tasks;
    coro::thread_pool tp{coro::thread_pool::options{.thread_count = 1}};
    const auto p1 = std::chrono::system_clock::now();
    auto double_task = [&](uint64_t x) -> coro::task<uint64_t> {
        co_await tp.schedule();
        co_return x * 2;
    };
    for (int i = 0; i < 1000000; i++) {
        tasks.emplace_back(double_task(i));
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "coro create cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    auto results = coro::sync_wait(coro::when_all(std::move(tasks)));
    const auto p3 = std::chrono::system_clock::now();
    std::cout << "coro complete cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p3 - p2).count() << "[µs]"
              << std::endl;
    // Create a task that awaits the doubling of its given value and
    // then returns the result after adding 5.
    auto double_and_add_5_task = [&](uint64_t input) -> coro::task<uint64_t> {
        auto doubled = co_await double_task(input);
        co_return doubled + 5;
    };

    auto output = coro::sync_wait(double_and_add_5_task(2));
    std::cout << "Task1 output = " << output << "\n";

    struct expensive_struct {
        std::string id{};
        std::vector<std::string> records{};

        expensive_struct() = default;
        ~expensive_struct() = default;

        // Explicitly delete copy constructor and copy assign, force only moves!
        // While the default move constructors will work for this struct the example
        // inserts explicit print statements to show the task is moving the value
        // out correctly.
        expensive_struct(const expensive_struct &) = delete;
        auto operator=(const expensive_struct &) -> expensive_struct & = delete;

        expensive_struct(expensive_struct &&other) : id(std::move(other.id)), records(std::move(other.records)) {
            std::cout << "expensive_struct() move constructor called\n";
        }
        auto operator=(expensive_struct &&other) -> expensive_struct & {
            if (std::addressof(other) != this) {
                id = std::move(other.id);
                records = std::move(other.records);
            }
            std::cout << "expensive_struct() move assignment called\n";
            return *this;
        }
    };

    // Create a very large object and return it by moving the value so the
    // contents do not have to be copied out.
    auto move_output_task = []() -> coro::task<expensive_struct> {
        expensive_struct data{};
        data.id = "12345678-1234-5678-9012-123456781234";
        for (size_t i = 10'000; i < 100'000; ++i) {
            data.records.emplace_back(std::to_string(i));
        }

        // Because the struct only has move contructors it will be forced to use
        // them, no need to explicitly std::move(data).
        co_return std::move(data);
    };

    auto data = coro::sync_wait(move_output_task());
    std::cout << data.id << " has " << data.records.size() << " records.\n";

    // std::unique_ptr<T> can also be used to return a larger object.
    auto unique_ptr_task = []() -> coro::task<std::unique_ptr<uint64_t>> { co_return std::make_unique<uint64_t>(42); };

    auto answer_to_everything = coro::sync_wait(unique_ptr_task());
    if (answer_to_everything != nullptr) {
        std::cout << "Answer to everything = " << *answer_to_everything << "\n";
    }
}

void TestPutAndGet(int start, rangedb::RingHashVec *ring_index) {
    const int key_size = 3 * 1000 * 1000;
    const auto p1 = std::chrono::system_clock::now();
    for (int i = start; i < key_size + start; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice.offset_ = i;
        slice.version_ = i;
        ring_index->insert(&slice);
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "skiplist insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    // ring_index->Print();
    const auto p3 = std::chrono::system_clock::now();
    for (int i = start; i < key_size + start; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice.offset_ = -1;
        slice.version_ = -1;
        bool result = ring_index->find(&slice);
        if (!result) {
            std::cout << "get key: " << std::to_string(i) << " not found" << std::endl;
        }
    }
    const auto p4 = std::chrono::system_clock::now();
    std::cout << "skiplist get time = " << std::chrono::duration_cast<std::chrono::microseconds>(p4 - p3).count() << "[µs]" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(100000));
    // range_skiplist.print();
}

void TestHash() {
    std::unordered_map<std::string, std::string> ring_index;
    const int key_size = 3 * 1000 * 1000;
    const auto p1 = std::chrono::system_clock::now();
    for (int i = 0; i < key_size; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice.offset_ = i;
        slice.version_ = i;
        ring_index.insert({key, key});
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "hash insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    const auto p3 = std::chrono::system_clock::now();
    for (int i = 0; i < key_size; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice slice = rangedb::Slice();
        slice.key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice.offset_ = -1;
        slice.version_ = -1;
        ring_index.find(key);
    }
    const auto p4 = std::chrono::system_clock::now();
    std::cout << "hash get time = " << std::chrono::duration_cast<std::chrono::microseconds>(p4 - p3).count() << "[µs]" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(100000));
    // range_skiplist.print();
}

void TestSetKey() {
    rangedb::RingHashVec *ring_index = new rangedb::RingHashVec();
    rangedb::Slice slice = rangedb::Slice();
    slice.key_ = rangedb::ByteKey((int8_t *)"key", 3);
    ring_index->insert(&slice);
    bool flag = ring_index->find(&slice);
    if (flag) {
        std::cout << "find key" << std::endl;
    } else {
        std::cout << "not find key" << std::endl;
    }
}

TEST(RangeSkiplistTest, base) {
    rangedb::RingHashVec *ring_index = new rangedb::RingHashVec();
    int start = 0;
    int parallel = 8;
    std::array<std::thread, 8> arr;
    for (int i = 0; i < parallel; i++) {
        std::thread t(TestPutAndGet, i * 3000000, ring_index);
        arr[i] = std::move(t);
    }
    for (int i = 0; i < parallel; i++) {
        arr[i].join();
    }
    // TestSetKey();
    // TestPutAndGet(start);
    // TestCoroTask();
    // TestHash();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}