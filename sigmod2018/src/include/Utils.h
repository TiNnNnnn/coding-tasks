#pragma once
#include <fstream>
#include <thread>
#include <iostream>
#include <mutex>
#include "Relation.h"
#include "MemoryPool.h"

//编译优化 
#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

#define CNT_PARTITIONS(WHOLE, PART) (((WHOLE) + ((PART)-1)) / (PART))
#define RADIX_HASH(value, base) ((value) & (base - 1))

//线程变量
extern MemoryPool **localMemPool;
extern thread_local int tid;
extern int nextTid;

extern unsigned cnt;
class Utils
{
public:
    // 创建一张空表
    static Relation createRelation(uint64_t size, uint64_t numColumns);

    // 将表落盘
    static void storeRelation(std::ofstream &out, Relation &r, unsigned i);

    // log2
    static int log2(unsigned v);

    // 获取当前线程ID
    static std::thread::id GetThreadId();
};

class AtomicCout
{
public:
    // 重载 << 运算符
    template <typename T>
    AtomicCout &operator<<(const T &obj)
    {
        std::lock_guard<std::mutex> lock(mutex); // 加锁
        std::cerr << obj;
        // 执行输出
        return *this;
    }

    // 重载无参数的 << 运算符，用于换行
    AtomicCout &operator<<(std::ostream &(*manip)(std::ostream &))
    {
        std::lock_guard<std::mutex> lock(mutex); // 加锁
        manip(std::cerr);                        // 执行输出操作
        return *this;
    }

private:
    std::mutex mutex; // 互斥量用于保护 std::cout
};

static AtomicCout atomic_cout;