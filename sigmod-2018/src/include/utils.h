#pragma once

#include <fstream>
#include <assert.h>
#include "mempool.h"
#include "relation.h"

// 宏变量：计算分区数量
#define CNT_PARTITIONS(WHOLE, PART) (((WHOLE) + ((PART)-1)) / (PART))
//宏变量：基于基数的哈希计算的宏
#define RADIX_HASH(value, base) ((value) & (base - 1))

// 线程变量
extern MemoryPool **localMemPool;
extern thread_local int tid;
extern int nextTid;

// 全局计数器
extern unsigned cnt;

class Utils
{
public:
    /// Create a dummy relation
    static Relation createRelation(uint64_t size, uint64_t num_columns);

    /// Store a relation in all formats
    static void storeRelation(std::ofstream &out, Relation &r, unsigned i);

    static void CondPanic(bool condition, const std::string &err)
    {
        if (!condition)
        {
            std::cout << err << std::endl;
            assert(condition);
        }
    }
};
