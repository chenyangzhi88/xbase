#pragma once
#include "db/index/HashTable.h"
#include "utils/InstructionTest.h"
#include "utils/Slice.h"
#include "xxhash.h"
#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <sys/mman.h>
#include <thread>

void test() {
    std::thread t1;
    const auto p1 = std::chrono::system_clock::now();
    m256_load_test();
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "hash insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
}

void test2() {
    std::map<std::string, bool> tmpMap;
    tmpMap["a"] = true;
    tmpMap["b"] = true;
    for (auto it = tmpMap.begin(); it != tmpMap.end(); it++) {
        std::cout << it->first << std::endl;
    }
    for (int i = 0; i < 10; i++) {
        std::cout << "i++" << i << std::endl;
    }

    for (int i = 0; i < 10; ++i) {
        std::cout << "++i" << i << std::endl;
    }
}

void test3() {
    unsigned short int vl_type = 0;
    std::unordered_map<std::string, std::string> tmpMap;
    const auto p1 = std::chrono::system_clock::now();
    for (int i = 0; i < 10000; i++) {
        tmpMap.insert(std::make_pair(std::to_string(i), std::to_string(i)));
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "hash insert time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    const auto p3 = std::chrono::system_clock::now();
    for (int i = 0; i < 10000; i++) {
        auto it = tmpMap.find(std::to_string(i));
    }
    const auto p4 = std::chrono::system_clock::now();
    std::cout << "hash find time = " << std::chrono::duration_cast<std::chrono::microseconds>(p4 - p3).count() << "[µs]" << std::endl;
    std::cout << "map size: " << tmpMap.size() << std::endl;
}

void test4() {
    std::string filename = "/home/milvus/db/tables/test_dog_v1__default__9/1703560010763124000/1703560010763124000.rv";
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cout << "Failed to open file " << filename << std::endl;
    }
    off_t merged_file_size = lseek(fd, 0, SEEK_END);
    if (merged_file_size == -1) {
        ::close(fd);
        std::cout << "Failed to lseek file " << filename << std::endl;
    }

    void *file_map_ptr = mmap(NULL, merged_file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (file_map_ptr == MAP_FAILED) {
        ::close(fd);
        std::cout << "Failed to mmap file " << filename << std::endl;
    }
    ::close(fd);
}

void test5(int argc, char **argv) {

    unsigned char hexBuffer[100] = {0};
    const auto p1 = std::chrono::system_clock::now();
    for (int i = 1; i < 100000; i++) {
        // SHA256 sha;
        std::string str = std::to_string(i);
        // hexBuffer[100]={0, };
        memcpy((char *)hexBuffer, str.c_str(), strlen(str.c_str()));
        XXH64(hexBuffer, strlen(str.c_str()), 0);
    }
    const auto p2 = std::chrono::system_clock::now();
    std::cout << "XXH64 cost time: " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" << std::endl;
    return;
    return;
}