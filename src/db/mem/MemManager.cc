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

#include "db/mem/MemManager.h"
#include "utils/Status.h"
#include "storage/block/Block.h"

namespace rangedb {
namespace db {

StatusCode MemManager::Get(Task* task) {
    auto source  = task->slice_;
    size_t block_id = source->offset_ / storage::BLOCK_SIZE;
    size_t block_offset = source->offset_ % storage::BLOCK_SIZE;
    storage::BlockPtr block = nullptr;
    bool exist = block_cache_.get(block_id, block);
    if (exist) {
        block->Read(source);
    } else {
        task->action_ = TaskType::GET_FROM_DISK;
    }
    return DB_SUCCESS;
}
}  // namespace db
}  // namespace rangedb
