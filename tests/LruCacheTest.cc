#include "utils/LruCache.h"
#include <gtest/gtest.h>
TEST(LRUCacheTest, base) {
    rangedb::LruCache<size_t, int> cache(10);
    for (int i = 0; i < 10; i++) {
        cache.put(i, i);
    }
    for (int i = 0; i < 10; i++) {
        int value = 0;
        cache.get(i, value);
        ASSERT_EQ(value, i);
    }
    cache.put(10, 10);
    cache.exists(11);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}