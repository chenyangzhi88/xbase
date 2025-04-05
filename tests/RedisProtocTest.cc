#include "server/RedisProtoc.h"
#include "gtest/gtest.h"

void RedisProtocTest() {
    {
        char *data = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        rangedb::Bytebuffer buffer(1024);
        rangedb::RedisDecoder decoder;
        rangedb::RedisRequest *request = nullptr;
        request = decoder.Decode(buffer);
        for (auto &arg : request->Args) {
            std::cout << arg.Value << std::endl;
        }
    }

    {
        char *data = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        rangedb::Bytebuffer buffer(1024);
        rangedb::RedisDecoder decoder;
        rangedb::RedisRequest *request = nullptr;
        request = decoder.Decode(buffer);
        for (auto &arg : request->Args) {
            std::cout << arg.Value << std::endl;
        }
    }
}

TEST(RedisProtocTest, base) {
    RedisProtocTest();
    // TestCoroTask();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}