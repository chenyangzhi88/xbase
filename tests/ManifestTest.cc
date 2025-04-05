#include "utils/Manifest.h"
#include "gtest/gtest.h"

using namespace rangedb;

void ManifestTest() {
    Manifest manifest;
    /*
    manifest.recode_num_ = 0;
    manifest.recode_offset_ = 1024;
    manifest.UpdateCheckpoint(100);
    manifest.UpdateKeyVersion(1);
    manifest.Sync();
    ASSERT_EQ(manifest.checkpoint_, 100);
    FileInfo file_info;
    file_info.file_id_ = 1;
    file_info.file_size_ = 100;
    file_info.block_num_ = 10;
    file_info.min_key_ = 1;
    file_info.max_key_ = 10;
    file_info.type = 0;
    file_info.status = 0;
    file_info.level = 0;
    int8_t buffer[1024];
    manifest.AppendFileRecode(&file_info);
    manifest.UpdateRecodeNum(manifest.recode_num_ + 1);
    */
    std::vector<FileInfo> file_info_vec;
    manifest.ReadFileRecode(&file_info_vec);
    manifest.Sync();
}

TEST(ManifestTest, base) { ManifestTest(); }

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}