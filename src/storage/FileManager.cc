#include "storage/FileManager.h"
#include "storage/sstblock/SstBlockFile.h"
#include "utils/Manifest.h"
#include "utils/Slice.h"
#include <cassert>
#include <cstdint>
#include <vector>

namespace rangedb {

FileManager *FileManager::instance_ = nullptr;
FileManager::FileManager(/* args */) {}
FileManager::~FileManager() {}

FileHandlePtr FileManager::CreateFile(const std::string &filename) {
    FileHandlePtr file_handle = std::make_shared<FileHandle>(filename);
    file_handle->Open();
    return file_handle;
}

storage::BlockFilePtr FileManager::GetBlockFile(uint32_t file_id) {
    if (block_files_.find(file_id) == block_files_.end()) {
        if (file_infos_.find(file_id) == file_infos_.end()) {
            return nullptr;
        }
        FileInfo *file_info = file_infos_[file_id];
        storage::BlockFilePtr block_file;
        block_file = std::make_shared<storage::SstBlockFile>(file_id);
        block_files_[file_id] = block_file;
    }
    return block_files_[file_id];
}

void FileManager::AddBlockFile(uint64_t file_id, storage::BlockFilePtr block_file) {
    block_files_[file_id] = block_file;
    // std::cout << "block file size: " << block_files_.size() << std::endl;
}

storage::BlockFilePtr FileManager::BinaryRangeSearch(const ByteKey &key, int level) {
    int left = 0;
    int mid = 0;
    if (sst_file_range_.size() <= level) {
        return nullptr;
    }
    std::vector<FileInfo *> &file_infos = sst_file_range_[level];
    int right = file_infos.size() - 1;
    while (left <= right) {
        mid = (left + right) / 2;
        ByteKey mid_min_key = file_infos[mid]->min_key_;
        ByteKey mid_max_key = file_infos[mid]->max_key_;
        if (key >= mid_min_key && key < mid_max_key) {
            return GetBlockFile(file_infos[mid]->file_id_);
        } else if (key < mid_min_key) {
            right = mid;
        } else if (key >= mid_max_key) {
            left = mid + 1;
        }
    }
    return nullptr;
}

void FileManager::GetLevelFile(int level, std::list<FileInfo *> *file_lst) {
    if (sst_file_range_.size() <= level) {
        return;
    }
    for (auto &file_info : sst_file_range_[level]) {
        file_lst->push_back(file_info);
    }
}

void FileManager::InitBlockFile(std::vector<FileInfo> &file_infos) {
    for (auto &file_info : file_infos) {
        storage::BlockFilePtr block_file;
        if (file_info.level == 0) {
            block_file = std::make_shared<WalBlockFile>(file_info.file_id_);
            size_t size = storage::SST_BLOCKFILE_HEADER_SIZE + 8 * 1024 * storage::BLOCK_SIZE;
            int8_t *data = new int8_t[size];
            FileHandlePtr file_handle = CreateFile(std::to_string(file_info.file_id_) + ".wal");
            file_handle->Read(data, size, 0);
            block_file->InitFromData(data);
        } else {
            block_file = std::make_shared<storage::SstBlockFile>(file_info.file_id_);
        }
        block_files_[file_info.file_id_] = block_file;
        AddFileInfo(file_info.file_id_, file_info.level, &file_info);
    }
}

void FileManager::AddFileInfo(uint64_t file_id, int level, FileInfo *file_info) {
    file_infos_[file_info->file_id_] = file_info;
    if (sst_file_range_.size() <= level) {
        sst_file_range_.resize(level + 1);
    }
    sst_file_range_.at(level).emplace_back(file_info);
}

void FileManager::AddLevelFile(int level) {}

} // namespace rangedb