
#include <string>

#include "db/index/MemRangeVector.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"

void TestPutAndGet() {
    rangedb::db::MemRangeVector mem_vector;
    const int key_size = 1 * 1000 * 1000;
    std::vector<rangedb::Task *> keys;
    coro::latch put_latch(key_size);
    coro::latch enqueue_latch(key_size);
    coro::thread_pool tp{coro::thread_pool::options{.thread_count = 1}};
    auto put_task = [&mem_vector, &keys, &put_latch, &tp, &enqueue_latch](int i) -> coro::task<void> {
        co_await tp.schedule();
        rangedb::Task *task = keys[i];
        task->action_ = rangedb::PUT_INDEX;
        mem_vector.put(task);
        enqueue_latch.count_down();
        co_await task->event_;
        // std::cout << "put key: " << std::to_string(i) << " done" << std::endl;
        put_latch.count_down();
        co_return;
    };
    std::vector<coro::task<void>> tasks{};
    for (int i = 0; i < key_size; i++) {
        std::string key = std::to_string(i);
        rangedb::Slice *slice = new rangedb::Slice();
        rangedb::Task *task = new rangedb::Task(slice);
        task->action_ = rangedb::PUT_INDEX;
        slice->key_ = rangedb::ByteKey((int8_t *)key.c_str(), key.length());
        slice->offset_ = i;
        slice->version_ = i;
        keys.emplace_back(task); // 将字符串存储在 vector 中
        tasks.emplace_back(put_task(i));
    }
    const auto p1 = std::chrono::system_clock::now();
    // const auto p_insert_thread = std::chrono::system_clock::now();
    // std::cout << "skiplist insert in queue time = " << std::chrono::duration_cast<std::chrono::microseconds>(p_insert_thread - p1).count() << "[µs]" << std::endl;
    auto wait_result_task = [&p1](coro::latch &latch) -> coro::task<void> {
        co_await latch;
        const auto p2 = std::chrono::system_clock::now();
        std::cout << "skiplist insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
        co_return;
    };
    auto queue_task = [&p1](coro::latch &latch) -> coro::task<void> {
        co_await latch;
        const auto p2 = std::chrono::system_clock::now();
        std::cout << "equeue time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
        co_return;
    };
    tasks.emplace_back(wait_result_task(put_latch));
    tasks.emplace_back(queue_task(enqueue_latch));
    coro::sync_wait(coro::when_all(std::move(tasks)));
    // while (mem_vector.GetCount() != key_size) {
    //     std::this_thread::sleep_for(std::chrono::milliseconds(1));
    // }

    const auto p3 = std::chrono::system_clock::now();
    for (int i = 0; i < key_size; i++) {
        rangedb::Slice *slice = keys[i]->slice_;
        keys[i]->action_ = rangedb::GET_INDEX;
        slice->offset_ = -1;
        slice->version_ = -1;
        bool result = mem_vector.get(keys[i]);
        if (!result) {
            // std::cout << "get key: " << std::to_string(i) << " not found" << std::endl;
        }
    }
    while (mem_vector.GetReadCount() != key_size) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto p4 = std::chrono::system_clock::now();
    std::cout << "skiplist get time = " << std::chrono::duration_cast<std::chrono::microseconds>(p4 - p3).count() << "[µs]" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(10000000));
    // range_skiplist.print();
}

TEST(MemRangeVectorTest, base) {
    TestPutAndGet();
    // TestCoroTask();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}