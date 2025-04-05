/*
 * File:   lrucache.h
 * Author: Alexander Ponomarev
 *
 * Created on June 20, 2013, 5:09 PM
 */

#ifndef _LRUCACHE_H_INCLUDED_
#define _LRUCACHE_H_INCLUDED_

#include <cstddef>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
namespace rangedb {
typedef std::shared_mutex Lock;
typedef std::unique_lock<Lock> WriteLock; // C++ 11
typedef std::shared_lock<Lock> ReadLock;  // C++ 14
template <typename key_t, typename value_t> class LruCache {
public:
    typedef typename std::pair<key_t, value_t> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

    LruCache(size_t max_size) : _max_size(max_size) {}

    void put(const key_t &key, const value_t &value) {
        WriteLock lock(lock_);
        auto it = _cache_items_map.find(key);
        _cache_items_list.push_front(key_value_pair_t(key, value));
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
        _cache_items_map[key] = _cache_items_list.begin();

        if (_cache_items_map.size() > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
        }
    }

    const bool get(const key_t &key, value_t &value) {
        WriteLock lock(lock_);
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            return false;
        } else {
            _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
            value = it->second->second;
            return true;
        }
    }

    bool exists(const key_t &key) {
        ReadLock lock(lock_);
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() {
        ReadLock lock(lock_);
        return _cache_items_map.size();
    }

private:
    std::list<key_value_pair_t> _cache_items_list;
    std::unordered_map<key_t, list_iterator_t> _cache_items_map;
    size_t _max_size;
    Lock lock_;
};

} // namespace rangedb

#endif /* _LRUCACHE_H_INCLUDED_ */