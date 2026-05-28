#ifndef KVROCKS_CACHE_CC
#define KVROCKS_CACHE_CC

#include <algorithm>
#include <unordered_map>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <cstring>
#include <mutex>
#include <shared_mutex>

template <typename K>
struct SliceHash {
    std::size_t operator()(const K& s) const 
    {
        return std::hash<std::string_view>{}(std::string_view(s.data(), s.size()));
    }
};

template <typename K1, typename K2>
struct SliceEquals {
    bool operator()(const K1& a, const K2& b) const 
    {
        if (a.size() != b.size()) {
            return false;
        }

        uint32_t len = a.size();
        uint32_t result = memcmp(a.data(),b.data(), len);
        return result == 0;
    }
};

template <typename K, typename V>
class KVRocksCache {
public:
    KVRocksCache(size_t capacity, size_t maxCount) : shards_(SHARD_NUM) {
        capacity_ = capacity;
        maxCount_ = maxCount;
        total_size_ = 0;
        total_count_ = 0;
    }
        
    KVRocksCache() : shards_(SHARD_NUM) {
        capacity_ = 0;
        maxCount_ = 0;
        total_size_ = 0;
        total_count_ = 0;
    }

    ~KVRocksCache() {}

    KVRocksCache(const KVRocksCache&) = delete;
    KVRocksCache& operator=(const KVRocksCache&) = delete;

    void SetCapacity(size_t capacity, size_t maxCount) {
        capacity_ = capacity;
        maxCount_ = maxCount;
        if (IsDisabled()) {
            clear();
        }
    }

    void Put(const K& key, const V& value) {
        if (IsDisabled()) {
            return;
        }
        auto& shard = getShard(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        shard.Put(key, value,
            ShardCapacityLimit(capacity_),
            ShardCountLimit(maxCount_));
    }

    V Get(const K& key) {
        if (IsDisabled()) {
            throw std::out_of_range("Key not found");
        }
        auto& shard = getShard(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.Get(key);
    }

    bool TryGet(const K& key, V* value) {
        if (IsDisabled()) {
            return false;
        }
        auto& shard = getShard(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.TryGet(key, value);
    }

    bool Contains(const K& key) {
        if (IsDisabled()) {
            return false;
        }
        auto& shard = getShard(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.Contains(key);
    }

    bool Remove(const K& key) {
        if (IsDisabled()) {
            return false;
        }
        auto& shard = getShard(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.Remove(key);
    }

    size_t Count() const {
        size_t total = 0;
        for (const auto& s : shards_) {
            std::lock_guard<std::mutex> lock(const_cast<Shard&>(s).mtx);
            total += s.count_;
        }
        return total;
    }

    size_t Size() const {
        size_t total = 0;
        for (const auto& s : shards_) {
            std::lock_guard<std::mutex> lock(const_cast<Shard&>(s).mtx);
            total += s.size_;
        }
        return total;
    }

    std::pair<K, V> GetOldestEntry() {
        // 注：全局最旧无法精确获取，这里取第一个非空分片最旧
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lock(s.mtx);
            if (s.count_ > 0) {
                return {s.tail->prev->key, s.tail->prev->value};
            }
        }
        throw std::out_of_range("empty");
    }

    void clear() {
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lock(s.mtx);
            s.clear();
        }
    }

    void traverse() {
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lock(s.mtx);
            auto list = s.entries();
            for (auto& [k, v] : list) {
                std::cout << k << " " << v << std::endl;
            }
        }
    }

    std::vector<std::pair<K, V>> entries() const {
        std::vector<std::pair<K, V>> res;
        for (const auto& s : shards_) {
            std::lock_guard<std::mutex> lock(const_cast<Shard&>(s).mtx);
            auto tmp = s.entries();
            res.insert(res.end(), tmp.begin(), tmp.end());
        }
        return res;
    }

private:
    struct Node {
        K key;
        V value;
        Node* prev;
        Node* next;
        //mutable std::mutex mtx;

        Node(const K& key, const V& value) : key(key), value(value), prev(nullptr), next(nullptr) {}
        Node() : prev(nullptr), next(nullptr) {}
    };

    // 单个分片 = 独立小LRU
    struct Shard {
        Shard() {
            head = new Node(K(), V());
            tail = new Node(K(), V());
            head->prev = nullptr;
            head->next = tail;
            tail->prev = head;
            tail->next = nullptr;
        }

        ~Shard() {
            clear();
            delete head;
            delete tail;
        }

        void removeNode(Node* node) {
            Node* prev = node->prev;
            Node* next = node->next;
    
            //std::lock(prev->mtx, node->mtx, next->mtx);
            prev->next = next;
            next->prev = prev;
        }
        
        void deleteNode(Node* node) {
            removeNode(node);
            size_t size = node->key.size() + node->value.size();
            delete node;
    
            //std::lock_guard<std::mutex> lock(stat_mtx_);
            count_--;
            size_ -= size;
        }
    
        void addToHead(Node* node) {
            Node* first = head->next;
    
            //std::lock(head_->mtx, first->mtx, node->mtx);
            node->next = first;
            node->prev = head;
            first->prev = node;
            head->next = node;
        }
    
        void addNode(Node* node) {
            addToHead(node);
            size_t size = node->key.size() + node->value.size();
    
            //std::lock_guard<std::mutex> lock(stat_mtx_);
            size_ += size;
            count_++;
        }
    
        void moveToHead(Node* node) {
            removeNode(node);
            addToHead(node);
        }

        // 内部实现
        void EnforceCapacityLocked(size_t capacity, size_t maxCount) {
            while((size_ > capacity && capacity != 0) || (count_ > maxCount && maxCount != 0)) {
                Node* node = tail->prev;
                map.erase(node->key);
                deleteNode(node);
            }
        }

        void Put(const K& key, const V& value, size_t capacity, size_t maxCount) {
            auto it = map.find(key);
            if(it != map.end()) {
                Node* node = it->second;
                size_t old_value_size = node->value.size();
                node->value = value;
                if (value.size() >= old_value_size) {
                    size_ += value.size() - old_value_size;
                } else {
                    size_ -= old_value_size - value.size();
                }
                moveToHead(node);
            } else {
                Node* node = new Node(key, value);
                map[key] = node;
                addNode(node);
            }
            EnforceCapacityLocked(capacity, maxCount);
        }

        V Get(const K& key) {
            auto it = map.find(key);
            if (it == map.end()) {
                throw std::out_of_range("Key not found");
            }
            Node* node = it->second;
            moveToHead(node);
            return node->value;
        }

        bool TryGet(const K& key, V* value) {
            auto it = map.find(key);
            if (it == map.end()) {
                return false;
            }
            Node* node = it->second;
            moveToHead(node);
            *value = node->value;
            return true;
        }

        bool Contains(const K& key) {
            return map.find(key) != map.end();
        }

        bool Remove(const K& key) {
            auto it = map.find(key);
            if (it == map.end()) return false;
            Node* node = it->second;
            map.erase(it);
            deleteNode(node);
            return true;
        }

        void clear() {
            while (head->next != tail) {
                deleteNode(head->next);
            }
            map.clear();
        }

        std::vector<std::pair<K, V>> entries() const {
            std::vector<std::pair<K, V>> res;
            Node* p = head->next;
            while (p != tail) {
                res.emplace_back(p->key, p->value);
                p = p->next;
            }
            return res;
        }

        std::mutex mtx;
        std::unordered_map<K, Node*, SliceHash<K>, SliceEquals<K, K>> map;
        Node* head;
        Node* tail;
        size_t count_ = 0;
        size_t size_ = 0;
    };

    static const int SHARD_NUM = 16;
    std::vector<Shard> shards_{SHARD_NUM};

    bool IsDisabled() const {
        return capacity_ == 0 && maxCount_ == 0;
    }

    static size_t ShardLimit(size_t limit) {
        if (limit == 0) {
            return 0;
        }
        return std::max<size_t>(1, (limit + SHARD_NUM - 1) / SHARD_NUM);
    }

    size_t ShardCapacityLimit(size_t capacity) const {
        return ShardLimit(capacity);
    }

    size_t ShardCountLimit(size_t maxCount) const {
        return ShardLimit(maxCount);
    }
    
    // 分片路由
    Shard& getShard(const K& key) {
        size_t h = SliceHash<K>{}(key);
        return shards_[h % SHARD_NUM];
    }

    size_t capacity_;
    size_t maxCount_;
    size_t total_size_;
    size_t total_count_;
};

#endif
