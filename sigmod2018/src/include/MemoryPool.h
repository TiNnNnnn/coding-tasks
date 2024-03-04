#pragma once
#include "stdint.h"
#include <tbb/concurrent_vector.h>
#include <stdint.h>
#include <stdlib.h>

struct buddy;
/*
    MmoeryPool using buddy
*/
class MemoryPool
{
public:
    struct buddy *meta;
    char *pool;

    // 分配單元大小
    uint64_t assignUnit;
    // 內存池大小
    uint64_t allocSize;

    tbb::concurrent_vector<void *> gcTarget;

    /*
        2*level만큼의 메모리 풀
        assign_unit단위로 할당
    */
    MemoryPool(uint64_t size, uint64_t assign_unit);
    ~MemoryPool();
    void *alloc(uint64_t size);
    void free(void *addr);
    // will be freed next alloc or destructor
    void requestFree(void *addr);
};

struct buddy *buddy_new(int level);
void buddy_delete(struct buddy *);
uint64_t buddy_alloc(struct buddy *, uint64_t size);
void buddy_free(struct buddy *, uint64_t offset);
uint64_t buddy_size(struct buddy *, uint64_t offset);
void buddy_dump(struct buddy *);