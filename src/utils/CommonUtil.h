
#pragma once

#include <cstdint>
#include <string>
#include <time.h>
#include <vector>

#include "utils/Status.h"

namespace rangedb {
class CommonUtil {
public:
    static Status CreateDirectory(const std::string &path);
    static std::string Uint128ToString(uint64_t file_id, uint32_t block_id);
};
} // namespace rangedb