#include "utils/FileHandle.h"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
namespace rangedb {

bool FileHandle::Open() {
    fd_ = open(filename_.c_str(), O_RDWR | O_CREAT, 0666);
    int error_code = errno;
    if (fd_ == -1) {
        std::cerr << "error: open file failed '" << filename_ << "'. cause by: " << strerror(error_code) << " (errno: " << error_code << ")"
                  << std::endl;
        return false;
    }
    return true;
}

bool FileHandle::Read(void *buffer, size_t size, off64_t offset) {
    off_t of = lseek(fd_, offset, SEEK_SET);
    if (of == -1) {
        return false;
    }
    ssize_t data_size = read(fd_, buffer, size);
    return data_size == size;
}

bool FileHandle::Write(const void *buffer, size_t size) {
    ssize_t write_size = write(fd_, buffer, size);
    if (write_size == -1) {
        return false;
    }
    // int error = fsync(fd_);
    return true;
}

bool FileHandle::WriteAt(const void *buffer, size_t size, off64_t offset) {
    off_t of = lseek(fd_, offset, SEEK_SET);
    if (of == -1) {
        return false;
    }
    ssize_t write_size = write(fd_, buffer, size);
    if (write_size == -1) {
        return false;
    }
    // int error = fsync(fd_);
    return true;
}

void FileHandle::Close() { close(fd_); }

void FileHandle::Sync() { fsync(fd_); }

void FileHandle::DeleteFile() {
    close(fd_);
    unlink(filename_.c_str());
}
} // namespace rangedb