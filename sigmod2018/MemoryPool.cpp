#include "MemoryPool.h"
#include <assert.h>
#include <iostream>
#include <Utils.h>
#include <sys/mman.h>

static inline uint64_t
next_pow_of_2(uint64_t x) {
	if (!(x & (x-1)))
		return x;
	x |= x>>1;
	x |= x>>2;
	x |= x>>4;
	x |= x>>8;
	x |= x>>16;
	x |= x>>32;
	return x+1;
}

MemoryPool::MemoryPool(uint64_t size, uint64_t assignUnit) {
    this->allocSize = next_pow_of_2(size);
    this->assignUnit = next_pow_of_2(assignUnit);
    uint64_t treeSize = this->allocSize/this->assignUnit;
    int level = 1;

    while ((treeSize>>=1)%2 != 1) {
        level++;
    }
    
    meta = buddy_new(level); 
    pool = (char*)malloc(allocSize);
    madvise(pool, allocSize, MADV_HUGEPAGE);    
}

MemoryPool::~MemoryPool() {
    buddy_delete(meta);
    ::free(pool);
}

void* MemoryPool::alloc(uint64_t size) {
    for (auto& g : gcTarget) {
        free(g);
    }
    gcTarget.clear();
    int off = buddy_alloc(meta, (size+assignUnit-1)/assignUnit);
    assert (off != -1);
    if (off == -1)
        return NULL;
    return (void*)(pool+off*assignUnit); 
}

void MemoryPool::free(void* addr) {
    assert(((uint64_t)addr) >= ((uint64_t)pool));
    assert(((uint64_t)addr) < ((uint64_t)pool)+allocSize);
    uint64_t offset = ((uint64_t)(addr)-(uint64_t)(pool))/assignUnit;
    buddy_free(meta, offset);
}

void MemoryPool::requestFree(void* addr) {
    gcTarget.push_back(addr);
}
