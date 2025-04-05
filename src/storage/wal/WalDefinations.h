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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace rangedb {
namespace storage {

#define UNIT_MB (1024 * 1024)
#define UNIT_B 1
#define LSN_OFFSET_MASK 0x00000000ffffffff

enum class MXLogType { None, Put, Delete, Flush };

struct MXLogRecord {
    uint64_t lsn;
    MXLogType type;
    std::string database;
    uint32_t data_size;
    const void *data = nullptr;
};

struct MXLogConfiguration {
    bool recovery_error_ignore;
    uint32_t buffer_size;
    std::string mxlog_path;
};

} // namespace storage
} // namespace rangedb
