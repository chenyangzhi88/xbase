#include "server/DB.h"
#include "storage/block/Block.h"
#include "storage/block/BlockFile.h"
#include "table/LsmTable.h"
#include "utils/Manifest.h"
#include "utils/Slice.h"
#include "utils/Status.h"
#include "utils/Task.h"
#include <cstdint>
namespace rangedb {

DB::DB(/* args */) {
    mem_vector_ = new RingHashVec();
    wal_put_thread_.detach();
    wal_manager_ = std::make_shared<WalManager>();
    block_manager_ = BlockManager::GetInstance();
    lsm_table_ = new LsmTable(mem_vector_);
    Init();
}

void DB::FlushWal() { wal_manager_->Flush(); }

Status DB::Put(Slice *source) {
    Status status = lsm_table_->Put(source);
    mem_vector_->insert(source);
    return status;
}

Status DB::Get(Slice *source) {
    TaskType type = TaskType::GET_INDEX;
    Task *task = new Task(source);
    task->action_ = type;
    bool exist = mem_vector_->find(task->slice_);
    Status status;
    if (!exist) {
        task->level_ = 2;
        status = lsm_table_->GetFromLevelFile(source, task);
        if (task->flag) {
            return status;
        } else {
            // not found
        }
    } else {
        Slice *slice = task->slice_;
        uint64_t file_id = slice->file_id_;
        if (slice->block_type_ == 0) {
            status = lsm_table_->GetFromMemBlock(slice);
            if (status.ok()) {
                task->done_ = true;
            } else {
                auto block = lsm_table_->GetFromLevelFile(source, task);
            }
        } else {
            storage::BlockFilePtr block_file = file_manager_->GetBlockFile(file_id);
            if (block_file != nullptr) {
                storage::BlockPtr block = block_manager_->GetBlock(file_id, slice->block_id_);
                if (block == nullptr) {
                    block_manager_->ReadBlock(slice);
                } else {
                    block->Read(slice);
                }
            } else {
                status = lsm_table_->GetFromLevelFile(source, task);
            }
        }
    }
    return status;  
}

void DB::AppendWal() {}

void DB::Init() {
    manifest_ptr_ = Manifest::ManifestGetInstance();
    std::vector<FileInfo> file_info_lst;
    manifest_ptr_->ReadFileRecode(&file_info_lst);
    file_manager_ = FileManager::GetInstance();
    file_manager_->InitBlockFile(file_info_lst);
    std::list<FileInfo *> file_lst;
    file_manager_->GetLevelFile(0, &file_lst);
    // init index cache for wal block and level 0 block
    for (auto &file_info : file_lst) {
        auto block = file_manager_->GetBlockFile(file_info->file_id_);
        auto iter = block->NewIterator(ByteKeyComparator());
        while (!iter->End()) {
            ByteKey key = iter->Key();
            Slice value = iter->Value();
            mem_vector_->insert(&value);
        }
    }
    file_lst.clear();
    file_manager_->GetLevelFile(1, &file_lst);
    for (auto &file_info : file_lst) {
        auto block = file_manager_->GetBlockFile(file_info->file_id_);
        auto iter = block->NewIterator(ByteKeyComparator());
        while (!iter->End()) {
            ByteKey key = iter->Key();
            Slice value = iter->Value();
            mem_vector_->insert(&value);
        }
    }
}
} // namespace rangedb
