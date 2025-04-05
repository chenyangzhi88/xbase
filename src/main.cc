#include "db/index/HashTable.h"
#include "db/mem/MemManager.h"
#include "utils/Slice.h"
#include "utils/Test.h"
#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <sys/mman.h>
#include <thread>

int main(int argc, char **argv) {
    rangedb::db::MemManager mem_manager;
    rangedb::Slice slice;
    int8_t *data = new int8_t[10];
    for (int i = 0; i < 10; i++) {
        data[i] = 'a';
    }
    slice.key_ = rangedb::ByteKey(data, 10);
    rangedb::Slice slice_1;
    slice_1.offset_ = 4;
    rangedb::Task *task = new rangedb::Task(&slice);
    mem_manager.Get(task);
    return 0;
}
