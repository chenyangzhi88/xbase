#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace rangedb {
namespace storage {

extern const char *WAL_META_FILE_NAME;

class MXLogMetaHandler {
public:
    explicit MXLogMetaHandler(const std::string &internal_meta_file_path);
    ~MXLogMetaHandler();

    bool GetMXLogInternalMeta(uint64_t &wal_lsn);

    bool SetMXLogInternalMeta(uint64_t wal_lsn);

private:
    FILE *wal_meta_fp_;
    uint64_t latest_wal_lsn_ = 0;
};

using MXLogMetaHandlerPtr = std::shared_ptr<MXLogMetaHandler>;

} // namespace storage
} // namespace rangedb