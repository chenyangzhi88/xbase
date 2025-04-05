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

#include <unistd.h>

#include <algorithm>
#include <memory>
#include <unordered_map>

#include "storage/wal/WalManager.h"
// #include "config/Config.h"
// #include "db/Options.h"
#include "utils/CommonUtil.h"

namespace rangedb {
namespace storage {

WalManager::WalManager(const MXLogConfiguration &config) {
    mxlog_config_.recovery_error_ignore = config.recovery_error_ignore;
    mxlog_config_.buffer_size = config.buffer_size;
    mxlog_config_.mxlog_path = config.mxlog_path;

    // check the path end with '/'
    if (mxlog_config_.mxlog_path.back() != '/') {
        mxlog_config_.mxlog_path += '/';
    }
    // check path exist
    auto status = CommonUtil::CreateDirectory(mxlog_config_.mxlog_path);
    if (!status.ok()) {
        std::string msg = "failed to create wal directory " + mxlog_config_.mxlog_path;
    }
}

WalManager::~WalManager() {}

ErrorCode WalManager::Init() {
    uint64_t applied_lsn = 0;
    p_meta_handler_ = std::make_shared<MXLogMetaHandler>(mxlog_config_.mxlog_path);
    if (p_meta_handler_ != nullptr) {
        p_meta_handler_->GetMXLogInternalMeta(applied_lsn);
    }

    uint64_t recovery_start = 0;
    // all tables are droped and a new wal path?
    if (applied_lsn < recovery_start) {
        applied_lsn = recovery_start;
    }

    ErrorCode error_code = WAL_ERROR;
    p_buffer_ = std::make_shared<MXLogBuffer>(mxlog_config_.mxlog_path, mxlog_config_.buffer_size);
    if (p_buffer_ != nullptr) {
        if (p_buffer_->Init(recovery_start, applied_lsn)) {
            error_code = WAL_SUCCESS;
        } else if (mxlog_config_.recovery_error_ignore) {
            p_buffer_->Reset(applied_lsn);
            error_code = WAL_SUCCESS;
        } else {
            error_code = WAL_FILE_ERROR;
        }
    }

    // buffer size may changed
    mxlog_config_.buffer_size = p_buffer_->GetBufferSize();

    last_applied_lsn_ = applied_lsn;
    return error_code;
}

ErrorCode WalManager::GetNextRecovery(MXLogRecord &record) {
    ErrorCode error_code = WAL_SUCCESS;
    while (true) {
        error_code = p_buffer_->Next(last_applied_lsn_, record);
        if (error_code != WAL_SUCCESS) {
            if (mxlog_config_.recovery_error_ignore) {
                // reset and break recovery
                p_buffer_->Reset(last_applied_lsn_);

                record.type = MXLogType::None;
                error_code = WAL_SUCCESS;
            }
            break;
        }
        if (record.type == MXLogType::None) {
            break;
        }

        // background thread has not started.
        // so, needn't lock here.
    }

    // print the log only when record.type != MXLogType::None
    if (record.type != MXLogType::None) {
    }

    return error_code;
}

ErrorCode WalManager::GetNextRecord(MXLogRecord &record) {
    auto check_flush = [&]() -> bool {
        std::lock_guard<std::mutex> lck(mutex_);
        if (flush_info_.IsValid()) {
            if (p_buffer_->GetReadLsn() >= flush_info_.lsn_) {
                // can exec flush requirement
                record.type = MXLogType::Flush;
                record.lsn = flush_info_.lsn_;
                flush_info_.Clear();
                return true;
            }
        }
        return false;
    };

    if (check_flush()) {
        return WAL_SUCCESS;
    }

    ErrorCode error_code = WAL_SUCCESS;
    while (WAL_SUCCESS == p_buffer_->Next(last_applied_lsn_, record)) {
        if (record.type == MXLogType::None) {
            if (check_flush()) {
                return WAL_SUCCESS;
            }
            break;
        }

        std::lock_guard<std::mutex> lck(mutex_);
    }

    if (record.type != MXLogType::None) {
    }
    return error_code;
}

bool WalManager::Put(Slice &slice) {
    MXLogType log_type;
    size_t head_size = SizeOfMXLogRecordHeader;

    MXLogRecord record;
    record.type = log_type;

    uint64_t new_lsn = 0;
    last_applied_lsn_ = new_lsn;
    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

bool WalManager::Del(Slice &slice) {
    size_t head_size = SizeOfMXLogRecordHeader;

    MXLogRecord record;
    record.type = MXLogType::Delete;

    uint64_t new_lsn = 0;
    last_applied_lsn_ = new_lsn;
    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

uint64_t WalManager::Flush() {
    std::lock_guard<std::mutex> lck(mutex_);
    // At most one flush requirement is waiting at any time.
    // Otherwise, flush_info_ should be modified to a list.
    __glibcxx_assert(!flush_info_.IsValid());

    uint64_t lsn = 0;
    if (lsn != 0) {
        flush_info_.lsn_ = lsn;
    }
    return lsn;
}

void WalManager::RemoveOldFiles(uint64_t flushed_lsn) {
    if (p_buffer_ != nullptr) {
        p_buffer_->RemoveOldFiles(flushed_lsn);
    }
}

} // namespace storage
} // namespace rangedb
