#pragma once
#include <stdint.h>
#include <stdlib.h>
#include "Utils.h"
#include "Concurrent_vector.h"

struct buddy;
class MemoryPool
{
public:
    struct buddy *meta;
    char *pool;

    // 分配單元大小
    uint64_t assignUnit;
    // 內存池大小
    uint64_t allocSize;

    concurrent_vector<void *> gcTarget;

    MemoryPool(uint64_t size, uint64_t assign_unit);
    ~MemoryPool();
    void *alloc(uint64_t size);
    void free(void *addr);
    // will be freed next alloc or destructor
    void requestFree(void *addr);
};

// buddy used C
struct buddy *buddy_new(int level);
void buddy_delete(struct buddy *);
uint64_t buddy_alloc(struct buddy *, uint64_t size);
void buddy_free(struct buddy *, uint64_t offset);
uint64_t buddy_size(struct buddy *, uint64_t offset);
void buddy_dump(struct buddy *);
