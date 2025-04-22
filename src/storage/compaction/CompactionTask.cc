#include "CompactionTask.h"
#include "storage/block/Block.h"
#include "storage/block/BlockFile.h"
#include "storage/sstblock/Merger.h"
#include "storage/sstblock/SstBlockFile.h"
#include "utils/Comparator.h"
#include "utils/Iterator.h"
#include "utils/Slice.h"
#include <cstddef>
#include <cstdint>
#include <list>
namespace rangedb {
void CompactionTask::Compact() {
    if (wal_compact_flag_) {
        WalBlockToCompaction();
    } else {
        SstBlockToCompaction();
    }
}

void CompactionTask::AddLowLevelFile(storage::BlockFilePtr file) { low_level_file_.push_back(file); }

void CompactionTask::AddHighLevelFile(storage::BlockFilePtr file) { high_level_file_.push_back(file); }

void CompactionTask::WalBlockToCompaction() {
    // 1. read wal block
    // 2. write to compaction block
    // 3. delete wal block
    std::list<Iterator *> file_lst;
    const Comparator *comparator = ByteKeyComparator();
    for (auto &file : low_level_file_) {
        auto block_file = std::dynamic_pointer_cast<WalBlockFile>(file);
        Iterator *iter = file->NewIterator(comparator);
        file_lst.push_back(iter);
    }
    for (auto &file : high_level_file_) {
        auto block_file = std::dynamic_pointer_cast<storage::SstBlockFile>(file);
        Iterator *iter = file->NewIterator(comparator);
        file_lst.push_back(iter);
    }
    Iterator *iter = storage::NewMergingIterator(cmp_, file_lst);
    iter->SeekToFirst();
    uint64_t file_id = storage::BlockFile::gloabal_block_id_.fetch_add(1);
    storage::SstBlockFile sst_file(file_id);
    bool flag = false;
    while (!iter->End()) {
        // construct two sst file
        Slice item = iter->Value();
        // if (flag) {
        // std::cout << "item: " << item.key_.ToString() << std::endl;
        //}
        auto status = sst_file.Append(&item);
        iter->Next();
        if (status == SST_BLOCK_FULL_ERROR) {
            // log error
            sst_file.Flush();
            file_id = storage::BlockFile::gloabal_block_id_.fetch_add(1);
            sst_file = storage::SstBlockFile(file_id);
            flag = true;
            sst_file.Append(&item);
        }
    }
    sst_file.Flush();
}

void CompactionTask::SstBlockToCompaction() {
    // 1. read sst block
    // 2. write to compaction block
    // 3. delete sst block
    while (!low_level_file_.empty() || !high_level_file_.empty()) {
        storage::BlockFilePtr h_file = high_level_file_.front();
        high_level_file_.pop_front();
        auto h_block_file = std::dynamic_pointer_cast<storage::SstBlockFile>(h_file);
        Iterator *h_iter = h_block_file->NewIterator(cmp_);
        storage::BlockFilePtr l_file = low_level_file_.front();
        auto l_block_file = std::dynamic_pointer_cast<storage::SstBlockFile>(l_file);
        low_level_file_.pop_front();
        Iterator *l_iter = l_block_file->NewIterator(cmp_);
        std::list<Iterator *> file_lst;
        file_lst.emplace_back(h_iter);
        file_lst.emplace_back(l_iter);
        Iterator *iter = storage::NewMergingIterator(cmp_, file_lst);
        std::vector<Slice> item_lst;
        size_t file_size = 0;
        while (!iter->End()) {
            item_lst.emplace_back(iter->Value());
            iter->Next();
        }
        size_t data_size = 0;
        if (data_size > 512 * 1024 * 1024) {
            // construct two sst file
            uint64_t file_id = storage::BlockFile::gloabal_block_id_.fetch_add(1);
            storage::SstBlockFile sst_file(file_id);
            for (auto &item : item_lst) {
                sst_file.Append(&item);
            }
            int block_num = sst_file.GetBlockNum();
            if (block_num > 256 * 16) {
                sst_file.Flush();
                sst_file = storage::SstBlockFile(file_id);
            }
        }
        // read sst block
        // write to compaction block
        // delete sst block
    }
}

} // namespace rangedb