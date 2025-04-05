#include "storage/block/BlockManager.h"

namespace rangedb {
std::shared_ptr<BlockManager> BlockManager::block_manager_instance = nullptr;
std::shared_ptr<BlockManager> BlockManager::GetInstance() {
    if (block_manager_instance == nullptr) {
        block_manager_instance = std::make_shared<BlockManager>();
    }
    return block_manager_instance;
}
} // namespace rangedb