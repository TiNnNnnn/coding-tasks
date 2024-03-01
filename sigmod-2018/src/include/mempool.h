#pragma once
#include "buddy.h"
#include "stdint.h"
#include <tbb/concurrent_vector.h>

/*
    MmoeryPool using buddy
*/
class MemoryPool
{
public:
    struct buddy *meta;
    char *pool;

    uint64_t assignUnit;
    uint64_t allocSize;

    tbb::concurrent_vector<void *> gcTarget;

    MemoryPool(uint64_t size, uint64_t assign_unit);
    ~MemoryPool();
    void *alloc(uint64_t size);
    void free(void *addr);
    // will be freed next alloc or destructor
    void requestFree(void *addr);
};