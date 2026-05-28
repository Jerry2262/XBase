#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include "kvrocks_cache.h"
#include "slice.h"

// 原子计数器，多线程安全统计
std::atomic<long long> correctCnt{0};
std::atomic<long long> errorCnt{0};
std::atomic<long long> NotFoundCnt{0};

// 多线程 Put 任务
void threadPut(KVRocksCache<Slice, std::string>& cache,
               const std::vector<std::string>& keyStrs,
               int start, int end) {
    for (int i = start; i < end; ++i) {
        Slice key(keyStrs[i]);
        std::string value = "value" + keyStrs[i];
        cache.Put(key, value);
    }
}

// 多线程 Get/Contains 任务
void threadGet(KVRocksCache<Slice, std::string>& cache, const std::vector<std::string>& keyStrs, int start, int end) {
    for (int i = start; i < end; ++i) {
    Slice key(keyStrs[i]);
    if (cache.Contains(key)) {
        std::string value1 = "value" + keyStrs[i];
        std::string value2 = cache.Get(key);
        if (value1 == value2)
            correctCnt++;
        else
            errorCnt++;
        } else {
            NotFoundCnt++;
        }
    }
}

std::string get_random_string(size_t length, int seed = 42) {
    const std::string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::mt19937 gen(seed);
    std::uniform_int_distribution<> dis(0, charset.size() - 1);
    
    std::string result;
    result.reserve(length);

    for (size_t i = 0; i < length; ++i) {
        result += charset[dis(gen)];
    }

    return result;
}

int main(int argc, char* argv[]) {
    int keyNum = 10000;
    size_t capacity = keyNum * 1024;
    size_t count = keyNum;
    size_t length = 32;
    if(argc == 2) {
        keyNum = atoi(argv[1]);
    } else if(argc == 5) {
        keyNum = atoi(argv[1]);
        capacity = atoi(argv[2]);
        count = atoi(argv[3]);
        length = atoi(argv[4]);
    } else {
        std::cout << "should have 0 or 1 or 4 para" << std::endl;
        return -1;
    }

    std::cout << "input keynum: " << keyNum << std::endl;
    std::cout << "input cache capacity: " << capacity << std::endl;
    std::cout << "input cache maxcount: " << count << std::endl;
    std::cout << "input key length: " << length << std::endl;

    KVRocksCache<Slice, std::string> cache(capacity, count);
    std::vector<std::string> keyStrs(keyNum);
    for(int i = 0; i < keyNum; i++) {
        keyStrs[i] = get_random_string(length, i);
    }

    // ====== 多线程 Put 测试 ======
    int threadNum = std::thread::hardware_concurrency() * 2; // 线程数 = CPU核心*2
    std::cout << "Use threads: " << threadNum << std::endl;

    auto putStart = std::chrono::steady_clock::now();

    std::vector<std::thread> putThreads;
    int batch = keyNum / threadNum;

    for (int i = 0; i < threadNum; ++i) {
        int start = i * batch;
        int end = (i == threadNum - 1) ? keyNum : (i + 1) * batch;
        putThreads.emplace_back(threadPut, std::ref(cache), std::cref(keyStrs), start, end);
    }

    for (auto& t : putThreads) t.join();

    auto putEnd = std::chrono::steady_clock::now();
    auto putCost = std::chrono::duration_cast<std::chrono::milliseconds>(putEnd - putStart).count();
    std::cout << "Put finish! time: " << putCost << "ms, QPS: " << keyNum * 1000.0 / putCost << std::endl;

    // ====== 多线程 Get 测试 ======
    auto getStart = std::chrono::steady_clock::now();

    std::vector<std::thread> getThreads;
    for (int i = 0; i < threadNum; ++i) {
        int start = i * batch;
        int end = (i == threadNum - 1) ? keyNum : (i + 1) * batch;
        getThreads.emplace_back(threadGet, std::ref(cache), std::cref(keyStrs), start, end);
    }

    for (auto& t : getThreads) t.join();

    auto getEnd = std::chrono::steady_clock::now();
    auto getCost = std::chrono::duration_cast<std::chrono::milliseconds>(getEnd - getStart).count();

    // ====== 输出结果 ======
    std::cout << "==================================" << std::endl;
    std::cout << "Get time: " << getCost << "ms, QPS: " << keyNum * 1000.0 / getCost << std::endl;
    std::cout << "cache capacity: " << capacity << ", size: " << cache.Size() << std::endl;
    std::cout << "cache maxcout:" << count << ", count: " << cache.Count() << std::endl;
    std::cout << "correct count: " << correctCnt << std::endl;
    std::cout << "error count: " << errorCnt << std::endl;
    std::cout << "not found count: " << NotFoundCnt << std::endl;
    return 0;
}
