#pragma once

#include <vector>

#include "db/index/RangeSkiplist.h"
#include "utils/Slice.h"
#include "utils/Task.h"

namespace rangedb {
namespace db {
class MemRangeVector {
private:
    std::vector<RangeSkiplist *> disk_range_nodes_;

public:
    MemRangeVector() {
        for (int i = 0; i < 8; i++) {
            RangeSkiplist *new_range = new RangeSkiplist(MIN_BYTE, MAX_BYTE);
            disk_range_nodes_.push_back(new_range);
        }
    }

    int64_t GetCount() {
        int64_t count = 0;
        for (auto node : disk_range_nodes_) {
            count += node->consumer_count;
        }
        return count;
    }

    int64_t GetReadCount() {
        int64_t count = 0;
        for (auto node : disk_range_nodes_) {
            count += node->consumer_read_count;
        }
        return count;
    }

    int64_t get(Task *task) {
        int index = std::abs(task->slice_->key_.hash_0_) % 8;
        return disk_range_nodes_[index]->find(task);
    }

    void put(Task *task) {
        int index = std::abs(task->slice_->key_.hash_0_ % 8);
        disk_range_nodes_[index]->insert(task);
    }
    int binaryRangeSearch(Slice *source) {
        int left = 0;
        int right = disk_range_nodes_.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            int flag = (disk_range_nodes_)[mid]->compare(source);
            switch (flag) {
            case 0:
                return mid;
                break;
            case -1:
                right = mid - 1;
                break;
            case 1:
                left = mid + 1;
                break;
            default:
                break;
            }
        }
        return -1;
    }
    void print() {
        std::vector<RangeSkiplist *>::iterator it = disk_range_nodes_.begin();
        for (it = disk_range_nodes_.begin(); it != disk_range_nodes_.end(); ++it) {
            (*it)->print();
        }
    }
};
} // namespace db
} // namespace rangedb