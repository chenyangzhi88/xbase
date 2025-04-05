#include <string>

#include "utils/Slice.h"
#include "gtest/gtest.h"

TEST(SliceTest, byte_type_test) {
    int8_t *data_1 = new int8_t[10];
    for (int i = 0; i < 10; i++) {
        data_1[i] = 'a';
    }
    rangedb::ByteKey b1(data_1, 10);
    int8_t *data_2 = new int8_t[9];
    for (int i = 0; i < 9; i++) {
        data_2[i] = 'a';
    }
    rangedb::ByteKey b2(data_2, 9);

    int8_t *data_3 = new int8_t[10];
    for (int i = 0; i < 10; i++) {
        data_3[i] = 'b';
    }
    rangedb::ByteKey b3(data_3, 10);

    rangedb::ByteKey b4 = b3;
    std::string s4((char *)b4.data_, b4.length_);
    EXPECT_EQ(s4, "bbbbbbbbbb");
    EXPECT_FALSE(b1 == b2);
    EXPECT_TRUE(b1 > b2);
    std::vector<rangedb::ByteKey *> vec;
    vec.push_back(&b3);
    vec.push_back(&b2);
    vec.push_back(&b1);
    std::sort(vec.begin(), vec.end(), [](rangedb::ByteKey *a, rangedb::ByteKey *b) { return *a < *b; });
    for (auto &b : vec) {
        std::string s((char *)b->data_, b->length_);
        std::cout << s << std::endl;
    }
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}