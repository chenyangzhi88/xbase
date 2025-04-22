#include "table/LsmTable.h"
#include "storage/block/Block.h"
#include "storage/sstblock/SstBlockFile.h"
#include "storage/walblock/WalBlock.h"
#include "utils/Iterator.h"
#include "utils/Manifest.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include <cstdint>
namespace rangedb {

LsmTable::LsmTable(RingHashVec *mem_vector) {
    mem_vector_ = mem_vector;
    db_file_id_ = 0;
    mutable_mem_block_ = new WalBlockFile(db_file_id_.fetch_add(1));
    file_manager_ = FileManager::GetInstance();
    BuildSstFile();
}

Status LsmTable::GetFromMemBlock(Slice *source) {
    if (source->file_id_ == mutable_mem_block_->GetFileId()) {
        storage::BlockPtr block = mutable_mem_block_->ReadBlock(source->block_id_);
        block->Read(source);
        return Status::OK();
    }
    for (auto &mem_file : unmutabl_mem_file_list_) {
        if (source->file_id_ == mem_file->GetFileId()) {
            storage::BlockPtr block = mem_file->ReadBlock(source->block_id_);
            block->Read(source);
            return Status::OK();
        }
    }
    return Status::Failed("not found");
}

Status LsmTable::GetFromLevelFile(Slice *source, Task *task) {
    // l1 read
    Status status;
    file_manager_->BinaryRangeSearch(source->key_, task->level_);
    return status;
}

Status LsmTable::Put(Slice *source) {
    if (mutable_mem_block_->remind() < source->Size()) {
        unmutabl_mem_file_list_.emplace_back(mutable_mem_block_);
        mutable_mem_block_ = new WalBlockFile(db_file_id_.fetch_add(1));
    }
    mutable_mem_block_->Append(source);
    source->file_id_ = mutable_mem_block_->GetFileId();
    return Status::OK();
}

void LsmTable::Close() {
    unmutabl_mem_file_list_.emplace_back(mutable_mem_block_);
    close_flag_.store(true);
    build_thread_.join();
}

void LsmTable::FlushMemTable() {
    auto front = unmutabl_mem_file_list_.front();
    uint64_t file_id = db_file_id_.fetch_add(1);
    storage::SstBlockFilePtr new_sst_file = std::make_shared<storage::SstBlockFile>(file_id);
    file_manager_->AddBlockFile(file_id, new_sst_file);
    Iterator *iter = front->NewIterator(ByteKeyComparator());
    iter->SeekToFirst();
    while (!iter->End()) {
        Slice value = iter->Value();
        value.file_id_ = file_id;
        new_sst_file->Append(&value);
        mem_vector_->insert(&value);
        iter->Next();
    }
    iter->Value().Print();
    Iterator *sst_iter = new_sst_file->NewIterator(ByteKeyComparator());
    sst_iter->SeekToFirst();
    const ByteKey &min_key = sst_iter->Key();
    sst_iter->SeekToLast();
    const ByteKey &max_key = sst_iter->Key();
    FileInfo *file_info = new FileInfo();
    file_info->file_id_ = file_id;
    file_info->block_num_ = new_sst_file->GetBlockNum();
    file_info->max_key_ = max_key;
    file_info->min_key_ = min_key;
    file_manager_->AddFileInfo(file_id, 1, file_info);
    unmutabl_mem_file_list_.pop_front();
    new_sst_file->Flush();
    ManifestPtr manifest_ptr = Manifest::ManifestGetInstance();
    manifest_ptr->AppendFileRecode(file_info);
}

void LsmTable::BuildSstFile() {
    build_thread_ = std::thread([this]() {
        while (!this->close_flag_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            while (unmutabl_mem_file_list_.size() > 4) {
                FlushMemTable();
            }
        }
        while(!unmutabl_mem_file_list_.empty()) {
            FlushMemTable();
        }
    });
}
} // namespace rangedb