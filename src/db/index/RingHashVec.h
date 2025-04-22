#pragma once
#include <vector>

#include "db/index/HashTable.h"
#include "utils/Slice.h"
#include <cmath>
#include <shared_mutex>
#include <mutex>

namespace rangedb {
typedef std::shared_mutex Lock;
typedef std::unique_lock< Lock >  WriteLock; // C++ 11
typedef std::shared_lock< Lock >  ReadLock;  // C++ 14
class RingHashVec {
private:
    /* data */
    Lock reash_lock_;
    struct Node {
        db::HashTable* hash_table_;
        Lock put_get_lock_;
        void put(Slice* source) {
            //WriteLock lock(put_get_lock_);
            hash_table_->put(source);
        }
        bool get(Slice* source) {
            //ReadLock lock(put_get_lock_);
            return hash_table_->get(source);
        }
        size_t GetSize() {
            //ReadLock lock(put_get_lock_);
            return hash_table_->GetSize();
        }
        void split(uint64_t ring_level, int split_level, db::HashTable* new_table) {
            //WriteLock lock(put_get_lock_);
            hash_table_->Split(new_table, ring_level, split_level);
        }
        void Print() {
            //ReadLock lock(put_get_lock_);
            hash_table_->Print();
        }
    };
    struct SplitNode {
        std::vector<db::HashTable*> hash_tables_;
        int split_level_;
        Lock split_lock_;
        void put(uint64_t ring_level, Slice* source) {
            bool flag = false;
            {
                WriteLock lock(split_lock_);
                int shift_index = GetShiftIndex(source->key_.hash_0_, ring_level, split_level_);
                if (hash_tables_[shift_index]->GetSize() < 1000) {
                    hash_tables_[shift_index]->put(source);
                } else {
                    int64_t ring_index = GetRingIndex(source->key_.hash_0_, ring_level);
                    bool shift = false;
                    if (split_level_ == 0) {
                        shift = true;
                    } else {
                        if (shift_index % 2 == 1) {
                            if(hash_tables_[shift_index] != hash_tables_[shift_index - 1]) {
                                shift = true;
                            }
                        } else {
                            if (hash_tables_[shift_index] != hash_tables_[shift_index + 1]) {
                                shift = true;
                            }
                        }
                    }
                    hashSplit(ring_level, split_level_, shift_index, shift);
                    int shift_index = GetShiftIndex(source->key_.hash_0_, ring_level, split_level_);
                    hash_tables_[shift_index]->put(source);
                }
            }
        }
        bool get(uint64_t ring_level, Slice* source) {
            ReadLock lock(split_lock_);
            int shift_index = GetShiftIndex(source->key_.hash_0_, ring_level, split_level_);
            bool flag = hash_tables_[shift_index]->get(source);
            return flag;
        }
        void hashSplit(int ring_level, int shift_level, int split_index, bool shift) {
            db::HashTable* new_table = new db::HashTable();
            if (shift) {
                shift_level += 1;
                int node_num = 1 << shift_level;
                for (int i = node_num - 1; i >= 0; i--) {
                    hash_tables_[i] = hash_tables_[i >> 1];
                }
                hash_tables_[split_index * 2]->Split(new_table, ring_level, shift_level);
                hash_tables_[split_index * 2 + 1] = new_table;
                split_level_ = shift_level;
            } else {
                if (split_index % 2 == 1) {
                    split_index -= 1;
                }
                hash_tables_[split_index]->Split(new_table, ring_level, shift_level);
                hash_tables_[split_index + 1] = new_table;
            }
        }
        inline int64_t GetRingIndex(int64_t hash, int ring_level) {
            return std::abs(hash / (1 << (64 - ring_level))) % (1 << ring_level);
        }
        inline int GetShiftIndex(int64_t hash, int ring_level, int split_level) {
            int64_t ring_index = GetRingIndex(hash, ring_level);
            int shift_index = std::abs((hash / (1 << (64 - ring_level - split_level)) % (1 << (ring_level + split_level)) - ring_index * (1 << split_level)) % (1 << split_level));
            return shift_index;
        }

    };
    int ring_level_;
    std::vector<SplitNode*> hash_tables_;
public:
    RingHashVec(/* args */);
    ~RingHashVec();

    bool TryInsert(const Slice* slice);

    void insert(Slice* slice);

    bool find(Slice* slice);

    bool TryFind(Slice* slice);

    void Print();

    bool Rehash();
    /*
    It deletes the element containing
    searchKey, if it exists.
    */
    void erase(const ByteKey& searchKey);
};
    
} // namespace rangedb
