// Copyright (C) 2019-2020 rangedb. All rights reserved.
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

#include <atomic>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/wal/WalBuffer.h"
#include "storage/wal/WalDefinations.h"
#include "storage/wal/WalFileHandler.h"
#include "storage/wal/WalMetaHandler.h"
#include "utils/Error.h"
#include "utils/Slice.h"

namespace rangedb {
namespace storage {

class WalManager {
public:
    explicit WalManager(const MXLogConfiguration &config);
    ~WalManager();
    ErrorCode Init();
    ErrorCode GetNextRecovery(MXLogRecord &record);

    /*
     * Get next record
     * @param record[out]: record
     * @retval error_code
     */
    ErrorCode GetNextRecord(MXLogRecord &record);
    bool Put(Slice &slice);
    bool Del(Slice &slice);
    uint64_t Flush();
    void RemoveOldFiles(uint64_t flushed_lsn);

    /*
     * Get the LSN of the last inserting or deleting operation
     * @retval lsn
     */
    uint64_t GetLastAppliedLsn() { return last_applied_lsn_; }

private:
    WalManager operator=(WalManager &);

    MXLogConfiguration mxlog_config_;

    MXLogBufferPtr p_buffer_;
    MXLogMetaHandlerPtr p_meta_handler_;
    std::mutex mutex_;
    std::atomic<uint64_t> last_applied_lsn_;

    // if multi-thread call Flush(), use list
    struct FlushInfo {
        uint64_t lsn_ = 0;
        bool IsValid() { return (lsn_ != 0); }
        void Clear() { lsn_ = 0; }
    };
    FlushInfo flush_info_;
};

} // namespace storage
} // namespace rangedb
