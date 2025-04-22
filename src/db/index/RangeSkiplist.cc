#include "RangeSkiplist.h"
#include <random>
#include <thread>

//==============================================================================
// Class RangeSkiplist member implementations

namespace rangedb {
static int64_t generate_random() {
    std::random_device rd;  // obtain a random number from hardware
    std::mt19937 gen(rd()); // seed the generator
    std::uniform_int_distribution<> distr(0, 1000000000);
    return (int64_t)distr(gen);
}
namespace db {
RangeSkiplist::RangeSkiplist(const ByteKey &min_key, const ByteKey &max_key)
    : min_key_(min_key), max_key_(max_key), probability(0.5), maxLevel(MAX_LEVEL) {
    ByteKey headKey = min_key_;
    ByteKey nilKey = max_key_;
    ByteKey splitRange = headKey;
    RangeNode *head_node = new RangeNode(headKey, headKey, maxLevel + 1);
    SegmentNode *segment = new SegmentNode();
    segment->splitNode_ = head_node;
    segment->size = 0;
    segmentVec.push_back(segment);

    RangeNode *tail_node = new RangeNode(nilKey, nilKey, maxLevel + 1);
    segment = new SegmentNode();
    segment->splitNode_ = tail_node;
    segment->size = 0;
    segmentVec.push_back(segment);

    ByteKey minkey = segmentVec[0]->splitNode_->max_key_;
    ByteKey maxkey = segmentVec[1]->splitNode_->min_key_;
    RangeNode *head = segmentVec[0]->splitNode_;
    RangeNode *end = segmentVec[1]->splitNode_;
    int level = randomLevel();
    RangeNode *startNode = new RangeNode(minkey, maxkey, level);
    std::vector<RangeNode *>::iterator it = head->forward.begin();
    std::advance(it, level);
    std::fill(head->forward.begin(), it, startNode);
    std::fill(it, head->forward.end(), end);
    std::fill(startNode->forward.begin(), startNode->forward.end(), end);
    segmentVec[0]->size += 1;
    // last node
    balanceSize = 100;
    delta_time = 0;
    level_iterate = 0;
    list_iterate = 0;
    split_count = 0;
    thread_ = std::thread(&RangeSkiplist::run, this);
    thread_.detach();
}

RangeSkiplist::~RangeSkiplist() {
    auto node = segmentVec[0]->splitNode_;
    while (node->forward[0]) {
        auto tmp = node;
        node = node->forward[0];
        delete tmp;
    }
    delete node;
}

bool RangeSkiplist::TryFind(Slice *source) {
    int index = binaryRangeSearch(source->key_);
    auto head = segmentVec[index];
    auto x = lower_bound(source->key_, head->splitNode_);
    x->find(source);
    return true;
}

const int64_t RangeSkiplist::find(Task *find_task) {
    queue_.enqueue(find_task);
    return 0;
}

void RangeSkiplist::iterate() const {
    RangeNode *list = segmentVec[0]->splitNode_;
    const ByteKey *preminkey;
    const ByteKey *premaxKey;
    while (list != nullptr) {

        const ByteKey &currentminKey = list->min_key_;
        const ByteKey &currentmaxKey = list->max_key_;
        if (currentminKey != *premaxKey) {
            std::cout << "can't iterate" << std::endl;
            print();
        }
        preminkey = &currentminKey;
        premaxKey = &currentmaxKey;
        // list->print();
        list = list->forward[0];
    }
}

void RangeSkiplist::print() const {
    RangeNode *list = segmentVec[0]->splitNode_;
    for (auto segment : segmentVec) {
        std::cout << "Segment minkey: " << segment->splitNode_->min_key_.ToString()
                  << ", maxkey: " << segment->splitNode_->max_key_.ToString() << ", size: " << segment->size << std::endl;
    }
    std::cout << "{" << std::endl;

    while (list != nullptr) {
        std::cout << "minkey: " << list->min_key_.ToString() << ", maxkey: " << list->max_key_.ToString() << ", level: " << nodeLevel(list)
                  << ", size: " << list->node_->GetSize() << std::endl;
        // list->print();
        list = list->forward[0];
    }
    std::cout << "}" << std::endl;
}

void RangeSkiplist::insert(Task *task) {
    queue_.enqueue(task);
    // tryInsert(searchKey, newValue);
    return;
}

bool RangeSkiplist::TryInsert(const Slice *source) {
    RangeNode *next;
    int currentSize;
    SegmentNode *head;

    // const auto p1 = std::chrono::system_clock::now();
    int index = binaryRangeSearch(source->key_);
    { // reassign value if node exists and return
        head = segmentVec[index];
        auto preds = predecessors(source->key_, head->splitNode_);
        next = preds[0]->forward[0];
        next->emplace(source, currentSize);
    }
    // const auto p2 = std::chrono::system_clock::now();
    // auto delta_time = std::chrono::duration_cast<std::chrono::nanoseconds>(p2 - p1).count();
    // std::cout << "hash insert delta time: " << delta_time << std::endl;
    {
        if (currentSize < PAGE_SIZE) {
            return true;
        }
        // const auto p2 = std::chrono::system_clock::now();
        // level_iterate += std::chrono::duration_cast<std::chrono::nanoseconds>(p2 - p1).count();
        const int newNodeLevel = randomLevel();
        {
            // const auto p3 = std::chrono::system_clock::now();
            RangeNode *newNodePtr = next->hashSplit(newNodeLevel);
            // const auto p4 = std::chrono::system_clock::now();
            // auto delta_time = std::chrono::duration_cast<std::chrono::nanoseconds>(p4 - p3).count();
            // std::cout << "hash insert delta time: " << delta_time << std::endl;
            // list_iterate += delta_time;
            auto preds = predecessors(source->key_, head->splitNode_);
            head->size++;
            split_count++;
            // connect pointers of predecessors and new node to respective successors
            for (int i = 0; i < newNodeLevel; ++i) {
                newNodePtr->forward[i] = preds[i]->forward[i];
                preds[i]->forward[i] = newNodePtr;
            }
        }
        return true;
    }
}

void RangeSkiplist::leftMoveSegmentNode(int index) {
    auto segment = segmentVec[index - 1];
    auto node = segment->splitNode_;
    uint32_t iterateCount = balanceSize / 2;
    for (int i = 0; i < segment->size - iterateCount; i++) {
        node = node->forward[0];
    }
    const ByteKey &splitKey = node->max_key_;
    auto preds = predecessors(splitKey, segmentVec[index - 1]->splitNode_);
    auto newSplitNode = new RangeNode(splitKey, splitKey, maxLevel + 1);
    for (int i = 0; i < nodeLevel(newSplitNode); i++) {
        newSplitNode->forward[i] = preds[i]->forward[i];
        preds[i]->forward[i] = newSplitNode;
    }
    auto movingSegment = segmentVec[index - 1];
    auto movingSplitNode = movingSegment->splitNode_;
    const ByteKey &movingKey = movingSplitNode->min_key_;
    preds = predecessors(movingKey, segmentVec[index - 1]->splitNode_);
    for (size_t i = 0; i < nodeLevel(movingSplitNode); ++i) {
        preds[i]->forward[i] = segment->splitNode_->forward[i];
    }
    segment->size -= iterateCount;
    segmentVec[index]->size += iterateCount;
    segmentVec[index]->splitNode_ = newSplitNode;
    return;
}

void RangeSkiplist::rightMoveSegmentNode(int index) {
    auto segment = segmentVec[index];
    auto node = segment->splitNode_;

    uint32_t iterateCount = balanceSize / 2;
    for (int i = 0; i < iterateCount; i++) {
        node = node->forward[0];
    }
    const ByteKey &splitKey = node->max_key_;
    auto preds = predecessors(splitKey, segmentVec[index]->splitNode_);
    auto newSplitNode = new RangeNode(splitKey, splitKey, maxLevel + 1);
    for (int i = 0; i < nodeLevel(newSplitNode); i++) {
        newSplitNode->forward[i] = preds[i]->forward[i];
        preds[i]->forward[i] = newSplitNode;
    }
    auto movingSegment = segmentVec[index];
    auto movingSplitNode = movingSegment->splitNode_;
    const ByteKey &movingKey = movingSplitNode->min_key_;
    preds = predecessors(movingKey, segmentVec[index - 1]->splitNode_);
    for (size_t i = 0; i < nodeLevel(movingSplitNode); ++i) {
        preds[i]->forward[i] = segment->splitNode_->forward[i];
    }
    segment->size -= iterateCount;
    segmentVec[index - 1]->size += iterateCount;
    segmentVec[index]->splitNode_ = newSplitNode;
    return;
}

void RangeSkiplist::erase(const ByteKey &searchKey) {
    auto preds = predecessors(searchKey, segmentVec[0]->splitNode_);

    // check if the node exists
    auto node = preds[0]->forward[0];
    if (node->max_key_ != searchKey || node == nullptr) {
        return;
    }

    // update pointers and delete node
    for (size_t i = 0; i < nodeLevel(node); ++i) {
        preds[i]->forward[i] = node->forward[i];
    }
    delete node;
}

// ###### private member functions ######
int RangeSkiplist::nodeLevel(const RangeNode *v) { return v->forward.size(); }

RangeSkiplist::RangeNode *RangeSkiplist::makeNode(ByteKey min_key, ByteKey max_key, std::string val, int level) {
    return new RangeNode(min_key, max_key, level);
}

int RangeSkiplist::randomLevel() const {
    int v = 1;
    while (((double)std::rand() / RAND_MAX) < probability && v < maxLevel) {
        v++;
    }
    return v;
}

RangeSkiplist::RangeNode *RangeSkiplist::lower_bound(const ByteKey &source, RangeNode *x) const {
    for (unsigned int i = nodeLevel(x); i-- > 0;) {
        while (x->forward[i]->max_key_ <= source) {
            x = x->forward[i];
        }
    }
    return x->forward[0];
}

std::vector<RangeSkiplist::RangeNode *> RangeSkiplist::predecessors(const ByteKey &searchKey, RangeNode *head) {
    std::vector<RangeNode *> result(nodeLevel(head), nullptr);
    RangeNode *x = head;

    for (int i = nodeLevel(head) - 1; i >= 0; i--) {
        while (x->forward[i]->max_key_ <= searchKey) {
            x = x->forward[i];
        }
        result[i] = x;
    }
    return result;
}

void RangeSkiplist::predecessors(RangeNode *startNode, const ByteKey &searchKey, std::vector<RangeNode *> *result, int level) {
    RangeNode *x = startNode;
    // std::cout << "the level is" << level << std::endl;
    // if (nodeLevel(startNode) < 0 || nodeLevel(startNode) >= 25)
    //{
    //     std::cout << "the start node level is zero: " << nodeLevel(startNode) <<  "and the level is " << level << std::endl;
    // }
    for (int i = level; i >= 0; i--) {
        while (x->forward[i]->max_key_ < searchKey) {
            x = x->forward[i];
        }
        (*result)[i] = x;
    }
}

void RangeSkiplist::run() {
    std::cout << "thread start" << std::endl;
    while (true) {
        Task *item[100];
        size_t bulk_size = queue_.wait_dequeue_bulk(item, 100);
        for (int i = 0; i < bulk_size; i++) {
            // const auto p1 = std::chrono::system_clock::now();
            switch (item[i]->action_) {
            case PUT_INDEX:
                TryInsert(item[i]->slice_);
                consumer_count = consumer_read_count + 1;
                item[i]->done_ = true;
                item[i]->event_.set();
                break;
            case GET_INDEX:
                TryFind(item[i]->slice_);
                item[i]->done_ = true;
                item[i]->slice_->data_ = "sss";
                consumer_read_count = consumer_read_count + 1;
                item[i]->event_.set();
                break;
            default:
                break;
            }
        }
        // const auto p2 = std::chrono::system_clock::now();
        // std::cout << "range insert delta time = " << std::chrono::duration_cast<std::chrono::microseconds>(p2 - p1).count() << "[µs]" <<
        // std::endl;
    }
}
} // namespace db
} // namespace rangedb
