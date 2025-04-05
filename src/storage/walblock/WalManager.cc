#include "storage/walblock/WalManager.h"
#include "storage/walblock/WalBlockFile.h"
#include "utils/Manifest.h"
#include "utils/Slice.h"
#include "utils/Status.h"
namespace rangedb {

WalManager::WalManager() { file_manager_ = FileManager::GetInstance(); }

Status WalManager::CreateBlockFile(size_t first_block_id) {
    std::string file_name = std::to_string(first_block_id) + ".wal";
    current_file_info_ = new FileInfo();
    current_file_info_->file_id_ = first_block_id;
    current_file_info_->status = 0;
    current_file_info_->level = 0;
    file_manager_->AddFileInfo(first_block_id, 0, current_file_info_);
    ManifestPtr manifest_ptr = Manifest::ManifestGetInstance();
    manifest_ptr->AppendFileRecode(current_file_info_);
    current_block_file_ = std::make_shared<WalBlockFile>(first_block_id);
    return Status::OK();
}

StatusCode WalManager::Put(Slice *source) {
    std::unique_lock<std::mutex> lock(mutex_);
    size_t remind_size = 0;
    if (current_block_file_ != nullptr) {
        remind_size = current_block_file_->remind();
    }
    if (remind_size < source->Size()) {
        if (current_block_file_ != nullptr) {
            current_block_file_->Flush();
        }
        if (current_file_info_ != nullptr) {
            current_file_info_->block_num_ = current_block_file_->GetBlockNum();
            current_file_info_->max_key_ = current_block_file_->GetMaxKey();
            current_file_info_->min_key_ = current_block_file_->GetMinKey();
            current_file_info_->file_size_ = current_block_file_->GetBlockNum() * storage::BLOCK_SIZE;
        }
        auto status = CreateBlockFile(db_block_id_);
        if (status.code() != DB_SUCCESS) {
            return status.code();
        }
    }
    auto status = current_block_file_->Append(source, db_block_id_);
    return status;
}

StatusCode WalManager::Flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    auto status = current_block_file_->Flush();
    return status.code();
}
} // namespace rangedb