#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <cstdio>
#include <string>

class Slice {
 public:
    Slice() : data_(""), size_(0) {}
    Slice(const char* d, size_t n) : data_(d), size_(n) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }
    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const {return size_ == 0; }
    char operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }

    void clear() {
        data_ = "";
        size_ = 0;
    }

    void remove_prefix(size_t n) {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    void remove_suffix(size_t n) {
        assert(n <= size());
        size_ -= n;
    }

    std::string ToString(bool hex = false) const;

    bool DecodeHex(std::string* result) const;
    int compare(const Slice& b) const;
    bool starts_with(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

    bool ends_with(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
    }

    size_t difference_offset(const Slice& b) const;

    const char* data_;
    size_t size_;
};