#include "db/index/RangeSkiplist.h"
#include "coro/coro.hpp"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <iostream>

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
    std::cout << "coro complete cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p3 - p2).count() << "[µs]" << std::endl;
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

        expensive_struct(expensive_struct &&other) : id(std::move(other.id)), records(std::move(other.records)) { std::cout << "expensive_struct() move constructor called\n"; }
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

void TestPutAndGet() {
    rangedb::db::RangeSkiplist range_skiplist(rangedb::MIN_BYTE, rangedb::MAX_BYTE);
    const int key_size = 10 * 1000 * 1000;
    const auto p1 = std::chrono::system_clock::now();
    std::vector<rangedb::Task *> keys;
    for (int i = 0; i < key_size; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice *slice = new rangedb::Slice();
        rangedb::Task *task = new rangedb::Task(slice);
        task->action_ = rangedb::PUT_INDEX;
        slice->key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice->offset_ = i;
        slice->version_ = i;
        keys.emplace_back(task); // 将字符串存储在 vector 中
        range_skiplist.insert(task);
    }
    while (range_skiplist.GetConsumerCount() != key_size) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "skiplist insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    const auto p3 = std::chrono::system_clock::now();
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice *slice = keys[i]->slice_;
        keys[i]->action_ = rangedb::GET_INDEX;
        slice->offset_ = -1;
        slice->version_ = -1;
        bool result = range_skiplist.find(keys[i]);
        if (!result) {
            // std::cout << "get key: " << std::to_string(i) << " not found" << std::endl;
        }
    }
    while (range_skiplist.GetConsumerReadCount() != key_size) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto p4 = std::chrono::system_clock::now();
    std::cout << "skiplist get time = " << std::chrono::duration_cast<std::chrono::microseconds>(p4 - p3).count() << "[µs]" << std::endl;
    // range_skiplist.print();
}

TEST(RangeSkiplistTest, base) {
    TestPutAndGet();
    // TestCoroTask();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}