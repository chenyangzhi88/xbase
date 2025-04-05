#pragma once
#include <string>
#include <vector>

namespace rangedb {
// Like java's Bytebuffer
class Bytebuffer {
    size_t position_;
    size_t limit_;
    size_t capacity_;
    char *buf_;

public:
    Bytebuffer(unsigned int capacity) : position_(0), limit_(capacity), capacity_(capacity) { buf_ = new char[capacity]; }
    ~Bytebuffer() { delete buf_; }

    inline bool HasRemaining() const { return position_ < limit_; }
    inline size_t Remaining() const { return limit_ - position_; }

    inline char *data() { return buf_; }

    inline std::size_t size() const { return capacity_; }

    inline bool empty() const { return capacity_ == 0; }

    inline char Get() { return buf_[position_++]; }
    void Get(std::string *dest, int size) {
        dest->append(buf_ + position_, size);
        position_ += size;
    }

    void Clear() {
        position_ = 0;
        limit_ = capacity_;
    }
    void Flip() {
        limit_ = position_;
        position_ = 0;
    }
};

struct RedisArg {
    unsigned int Size;
    std::string Value;
    enum { sReadData, sReadCR, sReadLF, sArgDone };
    int state;

public:
    explicit RedisArg(unsigned int size) : Size(size), state(sReadData) { Value.reserve(size); }

    bool Read(Bytebuffer &buffer) { // return true iff this arg get all read
        int toRead;
        while (buffer.HasRemaining() && state != sArgDone) {
            switch (state) {
            case sReadData:
                toRead = std::min(Size - Value.size(), buffer.Remaining());
                buffer.Get(&Value, toRead);
                if (Value.size() == Size)
                    state = sReadCR;
                break;
            case sReadCR: // read \r
                buffer.Get();
                state = sReadLF;
                break;
            case sReadLF: // read \n
                buffer.Get();
                state = sArgDone;
                break;
            }
        }
        return state == sArgDone;
    }
};

class LineReader {
    char buf_[16]; // 16 is enough, for reading argument count, byte count
    int linebufferIdx;

public:
    LineReader() : linebufferIdx(0) {}

    const char *ReadLine(Bytebuffer &buffer) {
        while (buffer.HasRemaining()) {
            char c = buffer.Get();
            buf_[linebufferIdx++] = c;
            if (c == '\n') {
                buf_[linebufferIdx + 1] = '\0';
                linebufferIdx = 0; // reset
                return buf_;
            }
        }
        return nullptr;
    }
};

struct RedisRequest {
public:
    std::vector<RedisArg> Args;
    RedisRequest(int argc) : argc(argc), argReading(0) {}

    void Add(int size) {
        Args.emplace_back(size);
        ++argReading;
    }
    // return true iff the whole requet decoded
    bool Done() { return argReading == argc; }
    bool ReadArg(Bytebuffer &buffer) { return Args[argReading - 1].Read(buffer); }

private:
    const int argc;
    int argReading;
};

class RedisDecoder {
    enum { sReadArgCount, sReadArgLen, sReadArg, sAllRead };
    int state; // state machine

    LineReader lineReader; // a complete line may not receied yet, do the buf_fering needed
    RedisRequest *request;

public:
    RedisDecoder() : state(sReadArgCount) {}

    // return the request iff a complete request is the buf_fer, else return null
    RedisRequest *Decode(Bytebuffer &buffer) {
        const char *line;
        while (buffer.HasRemaining() && state != sAllRead) {
            switch (state) {
            case sReadArgCount:
                if (buffer.Get() != '*') {
                }

                if ((line = lineReader.ReadLine(buffer)) != nullptr) {
                    int argc = std::atoi(line);
                    if (argc >= 1) {
                        request = new RedisRequest(argc);
                        state = sReadArgLen;
                    }
                }
                break;
            case sReadArgLen:
                if ((line = lineReader.ReadLine(buffer)) != nullptr) {
                    int size = std::atoi(line + 1);
                    if (size > 0 && size < 8 * 1024 * 1024) { // TODO max arg 8M configurable
                        request->Add(size);
                        state = sReadArg;
                    }
                }
                break;
            case sReadArg:
                if (request->ReadArg(buffer)) {
                    state = request->Done() ? sAllRead : sReadArgLen;
                }
                break;
            }
        }
        return state == sAllRead ? request : nullptr;
    }

    void Reset() { state = sReadArgCount; }
};

} // namespace rangedb