#include "utils/CommonUtil.h"
#include "utils/Status.h"
#include <sys/stat.h>

namespace rangedb {

Status CommonUtil::CreateDirectory(const std::string &path) {
    if (path.empty()) {
        return Status::OK();
    }

    struct stat directory_stat;
    int status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return Status::OK(); // already exist
    }

    status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return Status::OK(); // already exist
    }

    int makeOK = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH);
    if (makeOK != 0) {
        return Status(SERVER_UNEXPECTED_ERROR, "failed to create directory: " + path);
    }

    return Status::OK();
}

std::string CommonUtil::Uint128ToString(uint64_t file_id, uint32_t block_id) {
    unsigned __int128 value = file_id << 32 | block_id;
    if (value == 0) {
        return "0";
    }

    std::string result;
    while (value > 0) {
        result += '0' + (value % 10); //
        value /= 10;                  //
    }

    std::reverse(result.begin(), result.end()); //
    return result;
}

} // namespace rangedb
