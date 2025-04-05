#include "concurrentqueue/blockingconcurrentqueue.h"
#include "coro/coro.hpp"
#include "spdlog/spdlog.h"
#include <gtest/gtest.h>
#include <iostream>

void CoroutineTest() {
    coro::event e;
    moodycamel::BlockingConcurrentQueue<std::string> wal_queue_;
    // These tasks will wait until the given event has been set before advancing.
    auto make_wait_task = [&wal_queue_](const coro::event &e, uint64_t i) -> coro::task<void> {
        // std::cout << "task " << i << " is waiting on the event...\n";
        std::string s;
        std::cout << "dequeue: " << s << std::endl;
        wal_queue_.wait_dequeue_bulk(&s, 1);
        co_await e;
        // std::cout << "task " << i << " event triggered, now resuming.\n";
        co_return;
    };

    // This task will trigger the event allowing all waiting tasks to proceed.
    auto make_set_task = [&wal_queue_](coro::event &e) -> coro::task<void> {
        // std::cout << "set task is triggering the event\n";
        std::string s = "hello";
        std::cout << "equeue: " << s << std::endl;
        wal_queue_.enqueue(s);
        e.set();
        co_return;
    };
    const auto p1 = std::chrono::system_clock::now();
    // Given more than a single task to synchronously wait on, use when_all() to execute all the
    // tasks concurrently on this thread and then sync_wait() for them all to complete.
    coro::sync_wait(coro::when_all(make_set_task(e), make_wait_task(e, 1)));
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "coro create cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
}

TEST(CoroutineTest, base) { CoroutineTest(); }

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}