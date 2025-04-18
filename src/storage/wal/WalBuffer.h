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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/wal/WalDefinations.h"
#include "storage/wal/WalFileHandler.h"
#include "utils/Error.h"

namespace rangedb {
namespace storage {

#pragma pack(push)
#pragma pack(1)

struct MXLogRecordHeader {
    uint64_t mxl_lsn; // log sequence number (high 32 bits: file No. inc by 1, low 32 bits: offset in file, max 4GB)
    uint8_t mxl_type; // record type, put, delete
    uint32_t data_size;
};

const uint32_t SizeOfMXLogRecordHeader = sizeof(MXLogRecordHeader);

#pragma pack(pop)

struct MXLogBufferHandler {
    uint32_t max_offset;
    uint32_t file_no;
    uint32_t buf_offset;
    uint8_t buf_idx;
};

using BufferPtr = std::shared_ptr<char[]>;

class MXLogBuffer {
public:
    MXLogBuffer(const std::string &mxlog_path, const uint32_t buffer_size);
    ~MXLogBuffer();

    bool Init(uint64_t read_lsn, uint64_t write_lsn);

    // ignore all old wal file
    void Reset(uint64_t lsn);

    // Note: record.lsn will be set inner
    ErrorCode Append(MXLogRecord &record);

    ErrorCode Next(const uint64_t last_applied_lsn, MXLogRecord &record);

    uint64_t GetReadLsn();

    bool ResetWriteLsn(uint64_t lsn);

    void SetFileNoFrom(uint32_t file_no);

    void RemoveOldFiles(uint64_t flushed_lsn);

    uint32_t GetBufferSize();

    uint32_t SurplusSpace();

private:
    uint32_t RecordSize(const MXLogRecord &record);

private:
    uint32_t mxlog_buffer_size_; // from config
    BufferPtr buf_[2];
    std::mutex mutex_;
    uint32_t file_no_from_;
    MXLogBufferHandler mxlog_buffer_reader_;
    MXLogBufferHandler mxlog_buffer_writer_;
    MXLogFileHandler mxlog_writer_;
};

using MXLogBufferPtr = std::shared_ptr<MXLogBuffer>;

} // namespace storage
} // namespace rangedb
