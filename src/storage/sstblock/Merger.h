#pragma once
#include <list>
namespace rangedb {
class Iterator;
class Comparator;
namespace storage {
Iterator *NewMergingIterator(const Comparator *comparator, std::list<Iterator*> &children);
} // namespace storage
} // namespace rangedb