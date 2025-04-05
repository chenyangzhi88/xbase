#pragma once
#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>

namespace rangedb {
class FileHandle {
public:
    FileHandle(const std::string &filename) : filename_(filename) {}
    bool Open();
    bool Read(void *buffer, size_t size, off64_t offset);
    bool Write(const void *buffer, size_t size);
    bool WriteAt(const void *buffer, size_t size, off64_t offset);
    void Close();
    void Sync();
    void DeleteFile();

private:
    std::string filename_;
    int fd_;
};
using FileHandlePtr = std::shared_ptr<FileHandle>;
} // namespace rangedb