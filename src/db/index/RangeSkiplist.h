#pragma once
#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "db/index/HashTable.h"
#include "utils/Slice.h"
#include "utils/Task.h"

namespace rangedb {
namespace db {

#define SEGMENT_SIZE 24
#define MAX_LEVEL 16
#define PAGE_SIZE 1024

inline static int KeyCompare(const ByteKey &left_key, const ByteKey &right_key) {
    if (left_key > right_key) {
        return 1;
    } else if (left_key < right_key) {
        return -1;
    } else {
        return 0;
    }
}

class RangeSkiplist {
public:
    RangeSkiplist();
    ~RangeSkiplist();
    RangeSkiplist(const ByteKey &min_key, const ByteKey &max_key);

    // non-modifying member functions

    /*
    It prints the key, value, level
    of each node of the skip list.

    Prints two nodes per line.
    */
    void print() const;
    void iterate() const;
    /*
    It searches the skip list and
    returns the element corresponding
    to the searchKey; otherwise it returns
    failure, in the form of null pointer.
    */
    const int64_t find(Task *slice);

    bool TryFind(Slice *slice);
    // modifying member functions

    /*
    It searches the skip list for elements
    with seachKey, if there is an element
    with that key its value is reassigned to the
    newValue, otherwise it creates and splices
    a new node, of random level.
    */
    bool TryInsert(const Slice *slice);

    void insert(Task *slice);

    /*
    It deletes the element containing
    searchKey, if it exists.
    */
    void erase(const ByteKey &searchKey);
    void leftMoveSegmentNode(int index);
    void rightMoveSegmentNode(int index);

    int64_t getDeltaTime() { return delta_time; }

    int64_t getLevelIterate() { return level_iterate; }

    int64_t getListIterate() { return list_iterate; }

    int getSplitCount() { return split_count; }

    bool isChangeRange(int64_t searchKey);

    int binaryRangeSearch(const ByteKey &key) {
        int left = 0;
        int right = segmentVec.size() - 1;
        int flag = 0;
        int mid = 0;
        while (left <= right) {
            mid = (left + right) / 2;
            ByteKey right_min_key = segmentVec[mid + 1]->splitNode_->min_key_;
            ByteKey left_min_key = segmentVec[mid]->splitNode_->min_key_;
            int left_compare = KeyCompare(left_min_key, key);
            int right_compare = KeyCompare(right_min_key, key);
            if (left_compare <= 0 && right_compare > 0) {
                flag = 0;
            } else if (left_compare > 0) {
                flag = -1;
            } else if (left_compare <= 0) {
                flag = 1;
            } else {
                flag = 0;
            }
            switch (flag) {
            case 0:
                return mid;
                break;
            case -1:
                right = mid;
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
    void run();
    inline int compare(const Slice *target) const {
        int flag = 0;
        if (KeyCompare(target->key_, max_key_) >= 0) {
            flag = 1;
        } else if (KeyCompare(target->key_, min_key_) < 0) {
            flag = -1;
        }
        return flag;
    }

    inline size_t GetConsumerCount() { return consumer_count; }

    inline size_t GetConsumerReadCount() { return consumer_read_count; }

public:
    volatile int64_t consumer_count = 0;
    volatile int64_t consumer_read_count = 0;

private:
    struct RangeNode {
        ByteKey min_key_;
        ByteKey max_key_;
        HashTable *node_;
        // pointers to successor nodes
        std::vector<RangeNode *> forward;

        RangeNode(const ByteKey &left, const ByteKey &right, int level) : min_key_(left), max_key_(right), forward(level, nullptr) {
            node_ = new HashTable();
            // std::cout << "max_bucket_count = " << node_.bucket_count() << std::endl;
            // std::cout << "max_load_factor = " << node_.load_factor() << std::endl;
        }
        ~RangeNode() { delete node_; }
        bool emplace(const Slice *slice, int &currentSize) {
            bool flag = false;
            int size = node_->GetSize();
            if (size <= PAGE_SIZE) {
                node_->put(slice);
            } else {
                flag = false;
            }
            currentSize = node_->GetSize();
            return flag;
        }

        const bool find(Slice *source) {
            auto flag = node_->get(source);
            return flag;
        }
        RangeNode *hashSplit(int level) {
            size_t size = node_->GetSize();
            std::pair<int, std::vector<Slice>> split_pair = node_->Split();
            int mid_index = split_pair.first;
            std::vector<Slice> split_vec = split_pair.second;
            Slice &mid = split_vec[mid_index];

            RangeNode *tmp_node = new RangeNode(min_key_, mid.key_, level);
            HashTable *new_table = new HashTable();
            tmp_node->node_ = new_table;
            for (int i = 0; i < mid_index; i++) {
                new_table->put(&split_vec[i]);
                node_->del(&split_vec[i]);
            }
            this->min_key_ = mid.key_;
            return tmp_node;
        }

        void merge(const RangeNode *rightNode) {}
        void print() { node_->Print(); }
    };

    struct SegmentNode {
        RangeNode *splitNode_;
        size_t size;
        inline int compare(const Slice *target) {
            int flag = 0;
            if (KeyCompare(target->key_, splitNode_->max_key_) >= 0) {
                flag = 1;
            } else if (KeyCompare(target->key_, splitNode_->min_key_) < 0) {
                flag = -1;
            }
            return flag;
        }
    };

    // Generates node levels in the range [1, maxLevel).
    int randomLevel() const;

    // Returns number of incoming and outgoing pointers
    static int nodeLevel(const RangeNode *v);

    // creates a node on the heap and returns a pointer to it.
    static RangeNode *makeNode(ByteKey minkey, ByteKey maxkey, std::string val, int level);

    // Returns the first node for which node->key < searchKey is false
    RangeNode *lower_bound(const ByteKey &source, RangeNode *x) const;

    /*
     * Returns a collection of Pointers to Nodes
     * result[i] hold the last node of level i+1 for which result[i]->key < searchKey is true
     */
    std::vector<RangeNode *> predecessors(const ByteKey &searchKey, RangeNode *head);

    void predecessors(RangeNode *startNode, const ByteKey &searchKey, std::vector<RangeNode *> *result, int level);

    void split();

    void merge();
    // data members
    const float probability;
    const int maxLevel;
    std::vector<SegmentNode *> segmentVec;
    int64_t delta_time;
    int64_t level_iterate;
    int64_t list_iterate;
    // std::atomic<int64_t> produce_count = 0;
    int balanceSize;
    int split_count;
    std::thread thread_;
    ByteKey min_key_;
    ByteKey max_key_;
    moodycamel::BlockingConcurrentQueue<Task *> queue_;
};
} // namespace db
} // namespace rangedb
