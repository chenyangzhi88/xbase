#include "utils/Manifest.h"
#include "utils/Slice.h"
#include "gtest/gtest.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>
using namespace rangedb;

class ManifestTestFixture : public ::testing::Test {
protected:
    std::string test_dir = "data"; // 测试目录
    std::string manifest_path;     // 清单文件路径
    // 使用 unique_ptr 以便自动清理，如果 Manifest 没有很好地处理关闭
    std::unique_ptr<Manifest> manifest_ptr;
    // 辅助函数：创建示例 FileInfo
    FileInfo CreateSampleFileInfo(uint64_t id, const std::string &min_key_str, const std::string &max_key_str, uint8_t level = 0) {
        FileInfo fi;
        fi.file_id_ = id;
        fi.file_size_ = 1024 * id;
        fi.block_num_ = id * 10;
        fi.min_key_ = ByteKey((const int8_t *)min_key_str.c_str(), min_key_str.length()); // 使用假定的 ByteKey 构造函数
        fi.max_key_ = ByteKey((const int8_t *)max_key_str.c_str(), min_key_str.length()); // 使用假定的 ByteKey 构造函数
        fi.type = 1;                                                                      // sst
        fi.status = 0;                                                                    // normal
        fi.level = level;
        return fi;
    }
    bool ReadManifestHeader(uint64_t &magic, uint64_t &db_ver, uint64_t &checkpoint, uint64_t &key_ver, uint64_t &recode_num) {
        std::ifstream file(manifest_path, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        char buffer[1024]; // 匹配 Manifest 中使用的尺寸
        file.read(buffer, 1024);
        // 需要至少 5 个 uint64_t 的大小
        if (file.gcount() < sizeof(uint64_t) * 5)
            return false;

        size_t offset = 0;
        std::memcpy(&magic, buffer, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&db_ver, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&checkpoint, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&key_ver, buffer + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        std::memcpy(&recode_num, buffer + offset, sizeof(uint64_t));
        return true;
    }
    // 辅助函数：根据有问题的逻辑计算预期的序列化大小
    size_t GetExpectedSerializedFileInfoSize() const {
        // 基于有问题的 Serialize：3*uint64 + 2*uint32 + 2*uint64(hash) + 2*sizeof(data_) + 3*uint8
        FileInfo dummy;                           // 用于获取 sizeof(dummy.min_key_.data_)
        return sizeof(uint64_t) * 3 +             // file_id, file_size, block_num
               sizeof(uint32_t) * 2 +             // min_key.length, max_key.length
               sizeof(uint64_t) * 2 +             // min_key.hash_0, max_key.hash_0
               sizeof(dummy.min_key_.data_) * 2 + // min_key.data, max_key.data (固定大小)
               sizeof(uint8_t) * 3;               // type, status, level
    }
    // 每个测试用例执行前的准备工作
    void SetUp() override {
        // 确保测试目录存在且为空
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
        std::filesystem::create_directory(test_dir);
        manifest_path = (std::filesystem::path(test_dir) / "manifest").string(); // 转换为 string

        manifest_ptr = std::make_unique<Manifest>();
        // 需要确保 manifest_ptr_->file_handle_ptr_ 指向正确的测试路径。
        // 这可能需要修改 Manifest 或 FileHandle 以便测试。
        // 为简单起见，现在假设构造函数或某个方法允许设置路径，
        // 或者 FileHandle 默认使用 "./manifest" 并且我们依赖 SetUp 清理该文件。
        // 一个更清晰的方法：修改 Manifest 构造函数/添加方法 或 修改 FileHandle 构造函数。
        // 让我们继续假设 FileHandle("manifest") 使用构造函数中设置的路径。
        // 如果 FileHandle 硬编码了 "./manifest"，这个测试夹具需要调整。
        // 重新创建 FileHandle 以指向正确的测试路径：
        manifest_ptr->file_handle_ptr_ = std::make_shared<FileHandle>(manifest_path.c_str());
        manifest_ptr->file_handle_ptr_->Open(); // 使用正确的路径重新打开
        manifest_ptr->Init();                   // 使用正确的文件句柄重新运行 Init
    }
    // 每个测试用例执行后的清理工作
    void TearDown() override {
        // 清理测试目录
        manifest_ptr.reset(); // 在删除目录前显式释放资源
        if (std::filesystem::exists(test_dir)) {
            try {
                std::filesystem::remove_all(test_dir);
            } catch (const std::filesystem::filesystem_error &e) {
                // 如果需要，处理清理过程中的潜在错误
                std::cerr << "清理测试目录时出错: " << e.what() << std::endl;
            }
        }
    }
};
// 测试用例 1: 初始化一个新的清单文件
TEST_F(ManifestTestFixture, InitNewManifest) {
    ASSERT_NE(manifest_ptr, nullptr); // 断言指针非空

    // 检查 Init() 在文件不存在时执行后的初始值
    EXPECT_EQ(manifest_ptr->db_version_, 0);  // 期望等于 0
    EXPECT_EQ(manifest_ptr->checkpoint_, 0);  // 期望等于 0
    EXPECT_EQ(manifest_ptr->key_version_, 0); // 期望等于 0
    EXPECT_EQ(manifest_ptr->recode_num_, 0);  // 期望等于 0

    // 验证文件已创建并且文件头已正确写入
    ASSERT_TRUE(std::filesystem::exists(manifest_path)); // 断言文件存在
    uint64_t magic, db_ver, checkpoint, key_ver, recode_num;
    ASSERT_TRUE(ReadManifestHeader(magic, db_ver, checkpoint, key_ver, recode_num)); // 断言读取头成功

    EXPECT_EQ(magic, MANIFEST_MAGIC); // 期望等于魔法数
    EXPECT_EQ(db_ver, 0);             // 期望等于 0
    EXPECT_EQ(checkpoint, 0);         // 期望等于 0
    EXPECT_EQ(key_ver, 0);            // 期望等于 0
    EXPECT_EQ(recode_num, 0);         // 期望等于 0
}

// 测试用例 2: 追加文件记录并读取回来
TEST_F(ManifestTestFixture, AppendAndReadFileRecords) {
    ASSERT_NE(manifest_ptr, nullptr);
    EXPECT_EQ(manifest_ptr->recode_num_, 0); // 期望开始时为空

    std::vector<FileInfo> original_records;
    original_records.push_back(CreateSampleFileInfo(1, "key001", "key100", 0));
    original_records.push_back(CreateSampleFileInfo(2, "key101", "key200", 0));
    original_records.push_back(CreateSampleFileInfo(3, "key050", "key150", 1)); // 重叠的键范围，不同层级

    // 追加记录
    for (const auto &record : original_records) {
        uint64_t expected_recode_num = manifest_ptr->recode_num_ + 1;
        manifest_ptr->AppendFileRecode(&record);
        // 验证 recode_num 已更新 (在内存中，可能也通过 UpdateRecodeNum 中的 Write 更新到磁盘)
        EXPECT_EQ(manifest_ptr->recode_num_, expected_recode_num);
    }
    manifest_ptr->Sync(); // 确保记录已写入磁盘

    // 读取记录回来
    std::vector<FileInfo> read_records;
    manifest_ptr->ReadFileRecode(&read_records);

    // 验证记录数量
    ASSERT_EQ(read_records.size(), original_records.size()); // 断言数量相等

    // 验证每条记录的内容
    for (size_t i = 0; i < original_records.size(); ++i) {
        EXPECT_EQ(read_records[i].file_id_, original_records[i].file_id_);
        EXPECT_EQ(read_records[i].file_size_, original_records[i].file_size_);
        EXPECT_EQ(read_records[i].block_num_, original_records[i].block_num_);
        EXPECT_EQ(read_records[i].type, original_records[i].type);
        EXPECT_EQ(read_records[i].status, original_records[i].status);
        EXPECT_EQ(read_records[i].level, original_records[i].level);
        // 使用重载的 operator== 比较 ByteKey
        EXPECT_EQ(read_records[i].min_key_, original_records[i].min_key_);
        EXPECT_EQ(read_records[i].max_key_, original_records[i].max_key_);
    }

    // 检查文件头中的 recode_num 是否正确持久化
    uint64_t magic, db_ver, checkpoint, key_ver, recode_num_hdr;
    ASSERT_TRUE(ReadManifestHeader(magic, db_ver, checkpoint, key_ver, recode_num_hdr));
    EXPECT_EQ(recode_num_hdr, original_records.size());

    // 验证文件大小是否与 头部 + 记录 相符
    // 注意: 这在很大程度上取决于 GetExpectedSerializedFileInfoSize 的正确性，
    // 如果 FileInfo::Size() 或序列化逻辑有细微错误，可能会失败。
    std::error_code ec;
    uint64_t file_size = std::filesystem::file_size(manifest_path, ec);
    ASSERT_FALSE(ec); // 断言没有错误
    // 计算预期大小：头部 (强制 1024 字节?) + 记录数 * 序列化大小
    // 头部的 WriteAt 强制写入 1024 字节。AppendFileRecode 根据 sizeof(FileInfo) 计算偏移量，
    // 这可能与实际序列化的大小不同。
    // 让我们基于从 FileInfo 字段推导出的实际序列化大小来计算。
    size_t expected_record_size = GetExpectedSerializedFileInfoSize();
    uint64_t expected_total_size = RECODE_START + original_records.size() * expected_record_size;
    // 实际文件大小可能由于块分配或头部固定的 1024 字节写入而更大。
    // 我们检查它是否至少是预期大小。
    EXPECT_GE(file_size, expected_total_size); // 期望大于等于
}

// 测试用例 3: 更新元数据字段
TEST_F(ManifestTestFixture, UpdateMetadata) {
    ASSERT_NE(manifest_ptr, nullptr);

    // 更新值
    uint64_t new_checkpoint = 12345;
    uint64_t new_key_version = 67890;

    manifest_ptr->UpdateCheckpoint(new_checkpoint);
    manifest_ptr->UpdateKeyVersion(new_key_version);
    manifest_ptr->Sync(); // 确保更新已持久化

    // 验证内存中的值
    EXPECT_EQ(manifest_ptr->checkpoint_, new_checkpoint);
    EXPECT_EQ(manifest_ptr->key_version_, new_key_version);

    // 通过直接读取文件头验证持久化的值
    uint64_t magic, db_ver, checkpoint, key_ver, recode_num;
    ASSERT_TRUE(ReadManifestHeader(magic, db_ver, checkpoint, key_ver, recode_num));
    EXPECT_EQ(checkpoint, new_checkpoint);
    EXPECT_EQ(key_ver, new_key_version);

    // 通过创建新的 Manifest 实例并读取来验证持久性
    manifest_ptr.reset(); // 释放第一个实例
    auto manifest2 = std::make_unique<Manifest>();
    // 重新指向文件句柄并重新初始化
    manifest2->file_handle_ptr_ = std::make_shared<FileHandle>(manifest_path.c_str());
    manifest2->file_handle_ptr_->Open();
    manifest2->Init();

    EXPECT_EQ(manifest2->checkpoint_, new_checkpoint);
    EXPECT_EQ(manifest2->key_version_, new_key_version);
    EXPECT_EQ(manifest2->recode_num_, 0); // 此测试中未添加记录
}

// 测试用例 4: 使用现有的清单文件进行初始化
TEST_F(ManifestTestFixture, InitExistingManifest) {
    // --- 步骤 1: 创建一个初始状态并关闭 ---
    uint64_t initial_checkpoint = 999;
    uint64_t initial_key_version = 888;
    FileInfo record1 = CreateSampleFileInfo(10, "aaa", "bbb");
    FileInfo record2 = CreateSampleFileInfo(20, "ccc", "ddd");

    manifest_ptr->UpdateCheckpoint(initial_checkpoint);
    manifest_ptr->UpdateKeyVersion(initial_key_version);
    manifest_ptr->AppendFileRecode(&record1);
    manifest_ptr->AppendFileRecode(&record2);
    manifest_ptr->Sync();
    manifest_ptr.reset(); // 关闭第一个实例

    // --- 步骤 2: 创建一个新实例并验证它加载了状态 ---
    auto manifest2 = std::make_unique<Manifest>();
    // 重新指向文件句柄并重新初始化
    manifest2->file_handle_ptr_ = std::make_shared<FileHandle>(manifest_path.c_str());
    manifest2->file_handle_ptr_->Open();
    manifest2->Init();

    EXPECT_EQ(manifest2->db_version_, 0); // db_version 未被更新
    EXPECT_EQ(manifest2->checkpoint_, initial_checkpoint);
    EXPECT_EQ(manifest2->key_version_, initial_key_version);
    EXPECT_EQ(manifest2->recode_num_, 2); // 应加载记录计数

    // 验证记录可以正确读回
    std::vector<FileInfo> read_records;
    manifest2->ReadFileRecode(&read_records);
    ASSERT_EQ(read_records.size(), 2); // 断言记录数为 2
    EXPECT_EQ(read_records[0].file_id_, record1.file_id_);
    EXPECT_EQ(read_records[0].min_key_, record1.min_key_);
    EXPECT_EQ(read_records[1].file_id_, record2.file_id_);
    EXPECT_EQ(read_records[1].max_key_, record2.max_key_);
}

// 测试用例 5: 测试 Sync 方法 (基本上只检查它是否运行无误)
TEST_F(ManifestTestFixture, SyncMethod) {
    ASSERT_NE(manifest_ptr, nullptr);
    // 添加一些数据以便 sync 有内容可刷新
    manifest_ptr->UpdateCheckpoint(1);
    FileInfo record = CreateSampleFileInfo(1, "sync_min", "sync_max");
    manifest_ptr->AppendFileRecode(&record);

    // 调用 Sync 并断言它不抛出异常 (如果 FileHandle 可能抛出异常的话)
    ASSERT_NO_THROW(manifest_ptr->Sync()); // 断言不抛出异常
}

int main(int argc, char **argv) {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}