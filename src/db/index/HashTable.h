#pragma once

#include "db/mem/Row.h"
#include "utils/Slice.h"
#include "xxhash.h"
#include <algorithm>
#include <bitset>
#include <cstring>
#include <emmintrin.h>
#include <iostream>
#include <ostream>
#include <string>
#include <tmmintrin.h>

namespace rangedb {
namespace db {
using GenerationType = uint8_t;
using h2_t = uint8_t; // lower 7 bits of the hash

enum class ctrl_t : int8_t {
    kEmpty = -128,  // 0b10000000
    kDeleted = -2,  // 0b11111110
    kSentinel = -1, // 0b11111111
};

extern const ctrl_t kEmptyGroup[32];
inline ctrl_t *EmptyGroup() {
    // Const must be cast away here; no uses of this function will actually write
    // to it, because it is only used for empty tables.
    return const_cast<ctrl_t *>(kEmptyGroup + 16);
}

const uint64_t loBits = 0x0101010101010101;
const uint64_t hiBits = 0x8080808080808080;

const int32_t k_group_size = 64;

// Extracts the H1 portion of a hash: 57 bits mixed with a per-table salt.
inline size_t H1(uint8_t *hash) {
    size_t val;
    std::memcpy(&val, hash, sizeof(size_t));
    return val >> 7;
}

// Extracts the H2 portion of a hash: the 7 bits not used for H1.
//
// These are used as an occupied control byte.
inline uint8_t H2(uint8_t hash) { return hash & 0x7F; }

// Helpers for checking the state of a control byte.
inline bool IsEmpty(ctrl_t c) { return c == ctrl_t::kEmpty; }
inline bool IsFull(ctrl_t c) { return c >= static_cast<ctrl_t>(0); }
inline bool IsDeleted(ctrl_t c) { return c == ctrl_t::kDeleted; }
inline bool IsEmptyOrDeleted(ctrl_t c) { return c < ctrl_t::kSentinel; }

struct Slot {
    uint32_t length_;
    uint64_t offset_;
    uint64_t file_id_;
    uint32_t block_id_;
    uint8_t block_type_;
    uint64_t version_;
    ByteKey key_;
    TableRow* row_;
};

struct Group {
    ctrl_t ctrl_[16];
    Slot slots_[16];
};

inline __m128i _mm_cmpgt_epi8_fixed(__m128i a, __m128i b) {
#if defined(__GNUC__) && !defined(__clang__)
    if (std::is_unsigned<char>::value) {
        const __m128i mask = _mm_set1_epi8(0x80);
        const __m128i diff = _mm_subs_epi8(b, a);
        return _mm_cmpeq_epi8(_mm_and_si128(diff, mask), mask);
    }
#endif
    return _mm_cmpgt_epi8(a, b);
}

struct GroupCtrl {
    static constexpr size_t kWidth = 16; // the number of slots per group

    explicit GroupCtrl(const ctrl_t *pos) { ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos)); }

    // Returns a bitmask representing the positions of slots that match hash.
    uint32_t Match(h2_t hash) const {
        auto match = _mm_set1_epi8(static_cast<char>(hash));
        return static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
    }

    // Returns a bitmask representing the positions of empty slots.
    uint32_t MaskEmpty() const {
        // This only works because ctrl_t::kEmpty is -128.
        auto match = _mm_set1_epi8(static_cast<char>(ctrl_t::kEmpty));
        return static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
    }

    // Returns a bitmask representing the positions of empty or deleted slots.
    uint32_t MaskEmptyOrDeleted() const {
        auto special = _mm_set1_epi8(static_cast<char>(ctrl_t::kSentinel));
        return static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)));
    }

    // Returns the number of trailing empty or deleted elements in the group.
    uint32_t CountLeadingEmptyOrDeleted() const {
        auto special = _mm_set1_epi8(static_cast<char>(ctrl_t::kSentinel));
        return __builtin_ctz(static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)) + 1));
    }

    void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t *dst) const {
        auto msbs = _mm_set1_epi8(static_cast<char>(-128));
        auto x126 = _mm_set1_epi8(126);
        auto res = _mm_or_si128(_mm_shuffle_epi8(x126, ctrl), msbs);
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), res);
    }

    __m128i ctrl;
};
// using GroupCtrl = GroupCtrlSse2Impl;

class HashTable {

private:
    Group groups[k_group_size];
    int size_;

public:
    HashTable() {
        for (int i = 0; i < k_group_size; i++) {
            std::memset(groups[i].ctrl_, -128, 16);
        }
        size_ = 0;
    }
    inline uint32_t fastModN(uint32_t x, uint32_t n) { return uint32_t((uint64_t(x) * uint64_t(n)) >> 32); }
    inline uint32_t probeStart(size_t h1, int groups) { return fastModN(uint32_t(h1), uint32_t(groups)); }
    // Has returns true if |key| is present in |m|.
    bool has(Slice *source) {
        size_t h = XXH64(source->key_.data_, source->key_.length_, 0);
        size_t h1 = h >> 7;
        h2_t h2 = h & 0x7F;
        uint32_t group_index = probeStart(h1, k_group_size);
        for (int i = 0; i < k_group_size; i++) {
            GroupCtrl g = GroupCtrl(groups[group_index].ctrl_);
            uint32_t matches = g.Match(h2);
            while (matches != 0) {
                int key_index = __builtin_ctz(matches);
                matches &= ~(1 << key_index);
                // if (digest ==  groups[group_index].slots_[key_index].sha256_hash_) {
                //     return true;
                // }
            }
            // |key| is not in group |g|,
            // stop probing if we see an empty slot
            matches = g.MaskEmpty();
            if (matches != 0) {
                return false;
            }
            group_index += 1; // linear probing
            if (group_index > k_group_size) {
                group_index = 0;
            }
        }
        return false;
    }

    inline int GetSize() { return size_; }

    void put(const Slice *source) {
        // size_t h = XXH64(source->key_.data_, source->key_.length_, 0);
        size_t h = source->key_.hash_0_;
        size_t h1 = h >> 7;
        h2_t h2 = h & 0x7F;
        uint32_t group_index = probeStart(h1, k_group_size);
        for (int i = 0; i < k_group_size; i++) {
            GroupCtrl g = GroupCtrl(groups[group_index].ctrl_);
            uint32_t matches = g.Match(h2);
            while (matches != 0) {
                int key_index = __builtin_ctz(matches);
                matches &= ~(1 << key_index);
                auto slot = &groups[group_index].slots_[key_index];
                if (source->key_ == slot->key_) {
                    slot->offset_ = source->offset_;
                    slot->version_ = source->version_;
                    slot->length_ = source->data_length_;
                    slot->file_id_ = source->file_id_;
                    slot->block_id_ = source->block_id_;
                    slot->block_type_ = source->block_type_;
                    return;
                }
            }
            // |key| is not in group |g|,
            // stop probing if we see an empty slot
            matches = g.MaskEmpty();
            if (matches != 0) {
                int key_index = __builtin_ctz(matches);
                Slot &slot = groups[group_index].slots_[key_index];
                slot.offset_ = source->offset_;
                slot.version_ = source->version_;
                slot.length_ = source->data_length_;
                slot.file_id_ = source->file_id_;
                slot.block_id_ = source->block_id_;
                slot.block_type_ = source->block_type_;
                slot.key_ = source->key_;
                groups[group_index].ctrl_[key_index] = (ctrl_t)h2;
                size_++;
                return;
            }
            group_index = (group_index + 1) % k_group_size; // linear probing
        }
    }
    bool get(Slice *source) {
        // size_t h = XXH64(source->key_.data_, source->key_.length_, 0);
        size_t h = source->key_.hash_0_;
        size_t h1 = h >> 7;
        h2_t h2 = h & 0x7F;
        uint32_t group_index = probeStart(h1, k_group_size);
        for (int i = 0; i < k_group_size; i++) {
            GroupCtrl g = GroupCtrl(groups[group_index].ctrl_);
            uint32_t matches = g.Match(h2);
            while (matches != 0) {
                int key_index = __builtin_ctz(matches);
                auto &slot = groups[group_index].slots_[key_index];
                if (source->key_ == slot.key_) {
                    source->offset_ = slot.offset_;
                    source->version_ = slot.version_;
                    source->data_length_ = slot.length_;
                    source->file_id_ = slot.file_id_;
                    source->block_id_ = slot.block_id_;
                    source->block_type_ = slot.block_type_;
                    return true;
                }
                matches &= ~(1 << key_index);
            }
            // |key| is not in group |g|,n
            // stop probing if we see an empty slot
            matches = g.MaskEmpty();
            if (matches != 0) {
                return false;
            }
            group_index = (group_index + 1) % k_group_size; // linear probing
        }
        return false;
    }

    void Print() {
        for (int i = 0; i < k_group_size; i++) {
            for (int j = 0; j < 16; j++) {
                if (groups[i].ctrl_[j] != ctrl_t::kEmpty) {
                    Slice *slice = new Slice();
                    Slot &slot = groups[i].slots_[j];
                    slice->offset_ = slot.offset_;
                    slice->version_ = slot.version_;
                    slice->data_length_ = slot.length_;
                    slice->file_id_ = slot.file_id_;
                    slice->block_id_ = slot.block_id_;
                    slice->block_type_ = slot.block_type_;
                    slice->key_ = slot.key_;
                    std::cout << "group_index: " << i << ", key_index: " << j;
                    std::cout << " ctrl: " << std::bitset<8>((int)groups[i].ctrl_[j]);
                    std::cout << ", key: " << std::string((char *)slice->key_.data_, slice->key_.length_) << std::endl;
                }
            }
        }
    }

    std::pair<int, std::vector<Slice>> Split() {
        std::vector<Slice> elements = ListAllElements();
        int mid_index = elements.size() / 2;
        std::nth_element(elements.begin(), elements.begin() + mid_index, elements.end(),
                         [](Slice &a, Slice &b) { return a.key_ < b.key_; });
        return std::make_pair(mid_index, elements);
    }

    void Split(HashTable *new_table, uint64_t ring_level, int split_level) {
        std::vector<Slice> elements = ListAllElements();
        for (auto &slice : elements) {
            int64_t hash = slice.key_.hash_0_;
            int64_t ring_index = std::abs(hash / (1 << (64 - ring_level))) % (1 << ring_level);
            int shift_index = std::abs(
                (hash / (1 << (64 - ring_level - split_level)) % (1 << (ring_level + split_level)) - ring_index * (1 << split_level)) %
                (1 << split_level) % 2);
            if (shift_index == 1) {
                new_table->put(&slice);
                this->del(&slice);
            }
        }
    }

    std::vector<Slice> ListAllElements() {
        std::vector<Slice> all_elements;
        for (int i = 0; i < k_group_size; i++) {
            for (int j = 0; j < 16; j++) {
                if (groups[i].ctrl_[j] != ctrl_t::kEmpty) {
                    Slice *slice = &all_elements.emplace_back();
                    Slot &slot = groups[i].slots_[j];
                    slice->offset_ = slot.offset_;
                    slice->version_ = slot.version_;
                    slice->data_length_ = slot.length_;
                    slice->file_id_ = slot.file_id_;
                    slice->block_id_ = slot.block_id_;
                    slice->block_type_ = slot.block_type_;
                    slice->key_ = slot.key_;
                }
            }
        }
        return all_elements;
    }

    bool del(Slice *source) {
        size_t h = source->key_.hash_0_;
        size_t h1 = h >> 7;
        h2_t h2 = h & 0x7F;
        uint32_t group_index = probeStart(h1, k_group_size);
        uint32_t delete_group_index = 0;
        uint32_t delete_key_index = 0;
        bool deleted = false;
        Slot *last_slot = nullptr;
        Slot *delete_slot = nullptr;
        for (int i = 0; i < k_group_size; i++) {
            GroupCtrl g = GroupCtrl(groups[group_index].ctrl_);
            if (!deleted) {
                uint32_t matches = g.Match(h2);
                while (matches != 0) {
                    int key_index = __builtin_ctz(matches);
                    matches &= ~(1 << key_index);
                    auto &cur_slot = groups[group_index].slots_[key_index];
                    if (cur_slot.key_ == source->key_) {
                        delete_key_index = key_index;
                        delete_group_index = group_index;
                        delete_slot = &groups[group_index].slots_[key_index];
                        deleted = true;
                        size_--;
                        uint32_t empty_matches = g.MaskEmpty();
                        groups[delete_group_index].ctrl_[delete_key_index] = ctrl_t::kEmpty;
                        // std::cout << "the delete slot group index: " << delete_group_index << ", key index: " << delete_key_index <<
                        // std::endl;
                        if (empty_matches != 0) {
                            return deleted;
                        }
                        break;
                    }
                }
            } else {
                uint32_t empty_matches = g.MaskEmpty();
                for (int j = 0; j < 16; j++) {
                    if (groups[group_index].ctrl_[j] == ctrl_t::kEmpty) {
                        continue;
                    }
                    Slot &slot = groups[group_index].slots_[j];
                    int64_t h = slot.key_.hash_0_;
                    size_t h1 = h >> 7;
                    h2_t h2 = h & 0x7F;
                    uint32_t cur_group_index = probeStart(h1, k_group_size);
                    if (delete_group_index < group_index) {
                        if (cur_group_index > delete_group_index && cur_group_index <= group_index) {
                            continue;
                        }
                    }
                    if (delete_group_index > group_index) {
                        if (cur_group_index > delete_group_index || cur_group_index <= group_index) {
                            continue;
                        }
                    }
                    Slot &next_slot = groups[group_index].slots_[j];
                    delete_slot->offset_ = next_slot.offset_;
                    delete_slot->version_ = next_slot.version_;
                    delete_slot->file_id_ = next_slot.file_id_;
                    delete_slot->block_id_ = next_slot.block_id_;
                    delete_slot->block_type_ = next_slot.block_type_;
                    delete_slot->length_ = next_slot.length_;
                    delete_slot->key_ = next_slot.key_;
                    groups[delete_group_index].ctrl_[delete_key_index] = (ctrl_t)h2;
                    delete_group_index = group_index;
                    delete_key_index = j;
                    groups[delete_group_index].ctrl_[delete_key_index] = ctrl_t::kEmpty;
                    delete_slot = &groups[delete_group_index].slots_[delete_key_index];
                    break;
                }
                // std::cout << "the delete slot group index: " << delete_group_index << ", key index: " << delete_key_index << std::endl;
                if (empty_matches != 0) {
                    break;
                }
            }

            group_index = (group_index + 1) % k_group_size; // linear probing
        }
        return deleted;
    }
    void Clear() {
        for (int i = 0; i < k_group_size; i++) {
            std::memset(groups[i].ctrl_, -128, 16);
        }
    }
};

} // namespace db
} // namespace rangedb