// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>

#include "storage/block/Block.h"
#include "storage/block/BlockFile.h"
#include "utils/Slice.h"
#include "utils/Status.h"

namespace rangedb {
class WalBlockFile : virtual public storage::BlockFile {
public:
    WalBlockFile(uint64_t file_id);
    ~WalBlockFile() = default;

public:
    storage::BlockPtr AddBlock();  

    StatusCode Append(Slice *source, uint32_t cur_block_id);

    StatusCode Append(Slice *source) override;

    Status Flush() override;

    int GetBlockNum() override { return block_num_; }

    size_t remind();

    storage::BlockPtr ReadBlock(uint32_t block_id);

    Iterator *NewIterator(const Comparator *comparator) override;

    ByteKey GetMinKey() override { return file_min_key_; }

    ByteKey GetMaxKey() override { return file_max_key_; }
    Status InitFromData(int8_t *data) override;
    inline uint64_t GetFileId() { return file_id_; }
    Status FoundKey(Slice *key, bool *found) override  { return Status::OK(); };
    Status MaybeExist(Slice *key, bool *exist) override { return Status::OK(); };
    Status Read(size_t inner_block_id, size_t offset, Slice *slice) override { return Status::OK(); }; 

private:
    class Iter;

private:
    uint64_t file_id_;
    uint64_t start_block_id_ = 0;
    std::vector<storage::BlockPtr> block_list_;
    uint32_t block_num_ = 0;
    uint64_t cur_block_id_ = 0;
    std::array<ByteKey, storage::MAX_BLOCK_NUM> bloc_key_range_;
    ByteKey file_max_key_;
    ByteKey file_min_key_;
}; // WalBlockFile
using WalBlockFilePtr = std::shared_ptr<WalBlockFile>;
} // namespace rangedb
