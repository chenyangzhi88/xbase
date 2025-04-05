#pragma once
#include "utils/FileHandle.h"
#include "utils/Slice.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <unistd.h>
#include <vector>

namespace rangedb {
const uint64_t RECODE_START = 1024;
const uint64_t MANIFEST_MAGIC = 0x01aef7;
struct FileInfo {
    uint64_t file_id_;
    uint64_t file_size_;
    uint64_t block_num_;
    ByteKey min_key_;
    ByteKey max_key_;
    uint8_t type;   // 0: mem, 1:sst 
    uint8_t status; // 0: normal, 1: deleted
    uint8_t level;
    FileInfo() : file_id_(0), file_size_(0), block_num_(0), min_key_(), max_key_(), type(0), status(0), level(0) {}
    ~FileInfo() {}
    void Serialize(int8_t *buffer) const {
        size_t offset = 0;
        std::memcpy(buffer, &file_id_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &file_size_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &block_num_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &min_key_.length_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(buffer + offset, &min_key_.hash_0_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, min_key_.data_, sizeof(max_key_.data_));
        offset += sizeof(max_key_.data_);
        std::memcpy(buffer + offset, &max_key_.length_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(buffer + offset, &max_key_.hash_0_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, max_key_.data_, sizeof(max_key_.data_));
        offset += sizeof(max_key_.data_);
        std::memcpy(buffer + offset, &type, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        std::memcpy(buffer + offset, &status, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        std::memcpy(buffer + offset, &level, sizeof(uint8_t));
    }
    void Deserialize(const int8_t *buffer) {
        size_t offset = 0;
        std::memcpy(&file_id_, buffer, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&file_size_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&block_num_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&min_key_.length_, buffer + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(&min_key_.hash_0_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(min_key_.data_, buffer + offset, sizeof(max_key_.data_));
        offset += sizeof(max_key_.data_);
        std::memcpy(&max_key_.length_, buffer + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(&max_key_.hash_0_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(max_key_.data_, buffer + offset, sizeof(max_key_.data_));
        offset += sizeof(max_key_.data_);
        std::memcpy(&type, buffer + offset, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        std::memcpy(&status, buffer + offset, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        std::memcpy(&level, buffer + offset, sizeof(uint8_t));
    }

    int Size() const { return sizeof(uint64_t) * 3 + sizeof(uint32_t) * 2 + min_key_.length_ + max_key_.length_ + 3 * sizeof(uint8_t); }
};
struct Manifest {
    uint64_t db_version_;
    uint64_t checkpoint_;
    uint64_t key_version_;
    uint64_t recode_num_;
    FileHandlePtr file_handle_ptr_;
    inline static std::shared_ptr<Manifest> instance = nullptr;
    /* data */
    Manifest(/* args */) {
        file_handle_ptr_ = std::make_shared<FileHandle>("manifest");
        file_handle_ptr_->Open();
        Init();
    }
    ~Manifest(){};

    void Init() {
        auto buffer = std::make_unique<int8_t[]>(1024);
        file_handle_ptr_->Read(buffer.get(), 1024, 0);
        uint64_t magic;
        std::memcpy(&magic, buffer.get(), sizeof(uint64_t));
        if (magic != MANIFEST_MAGIC) {
            db_version_ = 0;
            checkpoint_ = 0;
            key_version_ = 0;
            recode_num_ = 0;
            Write();
        } else {
            Read();
        }
    }

    void UpdateCheckpoint(uint64_t checkpoint) {
        checkpoint_ = checkpoint;
        Write();
    }
    void UpdateKeyVersion(uint64_t key_version) {
        key_version_ = key_version;
        Write();
    }
    void UpdateRecodeNum(uint64_t recode_num) {
        recode_num_ = recode_num;
        Write();
    }
    void Serialize(int8_t *buffer) const {
        size_t offset = 0;
        std::memcpy(buffer, &MANIFEST_MAGIC, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &db_version_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &checkpoint_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &key_version_, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(buffer + offset, &recode_num_, sizeof(uint64_t));
    }

    void Deserialize(int8_t *buffer) {
        uint64_t magic;
        std::memcpy(&magic, buffer, sizeof(uint64_t));
        if (magic != MANIFEST_MAGIC) {
            return;
        }
        size_t offset = sizeof(uint64_t);
        std::memcpy(&db_version_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&checkpoint_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&key_version_, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&recode_num_, buffer + offset, sizeof(uint64_t));
    }

    void Write() {
        auto buffer = std::make_unique<int8_t[]>(1024);
        Serialize(buffer.get());
        file_handle_ptr_->WriteAt(buffer.get(), 1024, 0);
    }

    void Read() {
        auto buffer = std::make_unique<int8_t[]>(1024);
        file_handle_ptr_->Read(buffer.get(), 1024, 0);
        Deserialize(buffer.get());
    }

    void Sync() { file_handle_ptr_->Sync(); }

    void AppendFileRecode(const FileInfo *new_file) {
        auto buffer = std::make_unique<int8_t[]>(1024);
        new_file->Serialize(buffer.get());
        file_handle_ptr_->WriteAt(buffer.get(), sizeof(*new_file), RECODE_START + recode_num_ * sizeof(FileInfo));
        UpdateRecodeNum(recode_num_ + 1);
    }

    void ReadFileRecode(std::vector<FileInfo> *file_recode_list_) {
        for (uint64_t i = 0; i < recode_num_; i++) {
            auto buffer = std::make_unique<int8_t[]>(1024);
            file_handle_ptr_->Read(buffer.get(), sizeof(FileInfo), RECODE_START + i * sizeof(FileInfo));
            FileInfo file_recode;
            file_recode.Deserialize(buffer.get());
            file_recode_list_->push_back(file_recode);
        }
    }
    static std::shared_ptr<Manifest> ManifestGetInstance() {
        if (instance == nullptr) {
            instance = std::make_shared<Manifest>();
        }
        return instance;
    }
};
using ManifestPtr = std::shared_ptr<Manifest>;
} // namespace rangedb