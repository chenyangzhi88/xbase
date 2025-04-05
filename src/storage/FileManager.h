#pragma once
#include "storage/block/BlockFile.h"
#include "utils/FileHandle.h"
#include "utils/Manifest.h"
#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

namespace rangedb {
class FileManager {
private:
    /* data */
    std::unordered_map<uint32_t, FileInfo *> file_infos_;
    std::unordered_map<uint32_t, storage::BlockFilePtr> block_files_;
    std::vector<std::vector<FileInfo *>> sst_file_range_;
    ManifestPtr manifest_ptr_;
    static FileManager *instance_;

public:
    FileManager(/* args */);

    ~FileManager();

    FileHandlePtr CreateFile(const std::string &filename);

    void OpenFile(const std::string &file_name);

    void CloseFileHandle(const std::string &filename);

    FileHandlePtr GetFileHandle(const std::string &file_name);

    void GetLevelFile(int level, std::list<FileInfo *> *file_lst);

    storage::BlockFilePtr BinaryRangeSearch(const ByteKey &key, int level);

    storage::BlockFilePtr GetBlockFile(uint32_t file_id);

    void AddFileHandle(uint64_t file_id, FileHandlePtr file_handle);

    void AddBlockFile(uint64_t file_id, storage::BlockFilePtr block_file);

    void AddFileInfo(uint64_t file_id, int level, FileInfo *file_info);

    void AddLevelFile(int level);

    int GetWalFileNum();

    void InitBlockFile(std::vector<FileInfo> &file_infos);

    static FileManager *GetInstance() {
        if (instance_ == nullptr) {
            instance_ = new FileManager();
        }
        return instance_;
    }
};
using FileManagerPtr = std::shared_ptr<FileManager>;
} // namespace rangedb