#include "gtest/gtest.h"
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>

void writeCache() {
    constexpr size_t size = 1024 * 1024 * 100; // 100 MB
    char *buffer = new char[size];
    memset(buffer, 'A', size);

    int fd = open("testfile", O_CREAT | O_WRONLY, 0644);

    auto start = std::chrono::high_resolution_clock::now();
    write(fd, buffer, size); // 写入高速缓冲区
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Write to page cache time: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
              << " microseconds." << std::endl;

    close(fd);
    delete[] buffer;
}

void WriteShm() {
    constexpr size_t size = 1024 * 1024 * 100; // 100 MB
    uint8_t *data = new uint8_t[size];
    memset(data, 'A', size);
    uint8_t *data1 = new uint8_t[size];
    // 创建共享内存对象
    int shm_fd = shm_open("/my_shm", O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, size); // 设置大小

    // 映射共享内存
    void *shm_ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    auto start = std::chrono::high_resolution_clock::now();
    std::memcpy(data1, data, size); // 写入共享内存
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Write to shared memory time: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
              << " microseconds." << std::endl;

    // 清理
    munmap(shm_ptr, size);
    shm_unlink("/my_shm");
}

TEST(DBTest, base) {
    writeCache();
    WriteShm();
    // TestPutOps(0, db);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}