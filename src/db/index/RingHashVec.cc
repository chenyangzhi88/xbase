#include "db/index/RingHashVec.h"
#include <cmath>
namespace rangedb {
RingHashVec::RingHashVec(/* args */) {
    ring_level_ = 12;
    int vec_size = 1 << ring_level_;
    hash_tables_.resize(vec_size);
    for (int i = 0; i < vec_size; i++) {
        hash_tables_[i] = new SplitNode();
        hash_tables_[i]->split_level_ = 0;
        for (int j = 0; j < 1; j++) {
            hash_tables_[i]->hash_tables_.resize(32);
            hash_tables_[i]->hash_tables_[j] = new db::HashTable();
        }
    }
}

RingHashVec::~RingHashVec() {}
void RingHashVec::insert(Slice *slice) {
    int64_t hash = slice->key_.hash_0_;
    int64_t ring_index = std::abs(hash / (1 << (64 - ring_level_))) % (1 << ring_level_);
    hash_tables_[ring_index]->put(ring_level_, slice);
}
bool RingHashVec::find(Slice *slice) {
    int64_t hash = slice->key_.hash_0_;
    int64_t ring_index = std::abs(hash / (1 << (64 - ring_level_))) % (1 << ring_level_);
    return hash_tables_[ring_index]->get(ring_level_, slice);
}
void Rehash() {}

void RingHashVec::Print() {
    for (int i = 0; i < hash_tables_.size(); i++) {
        std::cout << "ring index: " << i << std::endl;
        int t = 1 << hash_tables_[i]->split_level_;
        for (int j = 0; j < t; j++) {
            std::cout << "shift index: " << j << std::endl;
            std::cout << "size: " << hash_tables_[i]->hash_tables_[j]->GetSize() << std::endl;
            hash_tables_[i]->hash_tables_[j]->Print();
        }
    }
}

} // namespace rangedb