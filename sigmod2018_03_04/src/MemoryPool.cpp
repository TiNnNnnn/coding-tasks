#include "MemoryPool.h"
#include <assert.h>
#include <iostream>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include "Utils.h"

static inline uint64_t
next_pow_of_2(uint64_t x)
{
    if (!(x & (x - 1)))
        return x;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return x + 1;
}

MemoryPool::MemoryPool(uint64_t size, uint64_t assignUnit)
{
    this->allocSize = next_pow_of_2(size);
    this->assignUnit = next_pow_of_2(assignUnit);
    uint64_t treeSize = this->allocSize / this->assignUnit;

    int level = 1;

    while ((treeSize >>= 1) % 2 != 1)
    {
        level++;
    }

    meta = buddy_new(level);
    pool = (char *)malloc(allocSize);
    madvise(pool, allocSize, MADV_HUGEPAGE);
}

MemoryPool::~MemoryPool()
{
    buddy_delete(meta);
    ::free(pool);
}

void *MemoryPool::alloc(uint64_t size)
{
    for (auto &g : gcTarget)
    {
        free(g);
    }
    gcTarget.clear();
    int off = buddy_alloc(meta, (size + assignUnit - 1) / assignUnit);
    assert(off != -1);
    if (off == -1)
        return NULL;
    return (void *)(pool + off * assignUnit);
}

void MemoryPool::free(void *addr)
{
    assert(((uint64_t)addr) >= ((uint64_t)pool));
    assert(((uint64_t)addr) < ((uint64_t)pool) + allocSize);
    uint64_t offset = ((uint64_t)(addr) - (uint64_t)(pool)) / assignUnit;
    buddy_free(meta, offset);
}

void MemoryPool::requestFree(void *addr)
{
    gcTarget.push_back(addr);
}

#define NODE_UNUSED 0
#define NODE_USED 1
#define NODE_SPLIT 2
#define NODE_FULL 3

struct buddy
{
    int level;
    uint8_t tree[1];
};

struct buddy *
buddy_new(int level)
{
    uint64_t size = 1lu << level;
    struct buddy *self = (struct buddy *)malloc(sizeof(struct buddy) + sizeof(uint8_t) * (size * 2 - 2));
    self->level = level;
    madvise(self, sizeof(struct buddy) + sizeof(uint8_t) * (size * 2 - 2), MADV_HUGEPAGE);
    memset(self->tree, NODE_UNUSED, size * 2 - 1);
    return self;
}

void buddy_delete(struct buddy *self)
{
    free(self);
}

static inline int
is_pow_of_2(uint64_t x)
{
    return !(x & (x - 1));
}

static inline uint64_t
_index_offset(uint64_t index, int level, int max_level)
{
    return ((index + 1) - (1 << level)) << (max_level - level);
}

static void
_mark_parent(struct buddy *self, uint64_t index)
{
    for (;;)
    {
        int buddy = index - 1 + (index & 1) * 2;
        if (buddy > 0 && (self->tree[buddy] == NODE_USED || self->tree[buddy] == NODE_FULL))
        {
            index = (index + 1) / 2 - 1;
            self->tree[index] = NODE_FULL;
        }
        else
        {
            return;
        }
    }
}

uint64_t
buddy_alloc(struct buddy *self, uint64_t s)
{
    uint64_t size;
    if (s == 0)
    {
        size = 1;
    }
    else
    {
        size = (uint64_t)next_pow_of_2(s);
    }
    uint64_t length = 1 << self->level;

    if (size > length)
        return -1;

    uint64_t index = 0;
    int level = 0;

    while (index >= 0)
    {
        if (size == length)
        {
            if (self->tree[index] == NODE_UNUSED)
            {
                self->tree[index] = NODE_USED;
                _mark_parent(self, index);
                return _index_offset(index, level, self->level);
            }
        }
        else
        {
            // size < length
            switch (self->tree[index])
            {
            case NODE_USED:
            case NODE_FULL:
                break;
            case NODE_UNUSED:
                // split first
                self->tree[index] = NODE_SPLIT;
                self->tree[index * 2 + 1] = NODE_UNUSED;
                self->tree[index * 2 + 2] = NODE_UNUSED;
            default:
                index = index * 2 + 1;
                length /= 2;
                level++;
                continue;
            }
        }
        if (index & 1)
        {
            ++index;
            continue;
        }
        for (;;)
        {
            level--;
            length *= 2;
            index = (index + 1) / 2 - 1;
            if (index < 0)
                return -1;
            if (index & 1)
            {
                ++index;
                break;
            }
        }
    }

    return -1;
}

static void
_combine(struct buddy *self, uint64_t index)
{
    for (;;)
    {
        int buddy = index - 1 + (index & 1) * 2;
        if (buddy < 0 || self->tree[buddy] != NODE_UNUSED)
        {
            self->tree[index] = NODE_UNUSED;
            while (((index = (index + 1) / 2 - 1) >= 0) && self->tree[index] == NODE_FULL)
            {
                self->tree[index] = NODE_SPLIT;
            }
            return;
        }
        index = (index + 1) / 2 - 1;
    }
}

void buddy_free(struct buddy *self, uint64_t offset)
{
    assert(offset < (1lu << self->level));
    uint64_t left = 0;
    uint64_t length = 1lu << self->level;
    uint64_t index = 0;

    for (;;)
    {
        switch (self->tree[index])
        {
        case NODE_USED:
            assert(offset == left);
            _combine(self, index);
            return;
        case NODE_UNUSED:
            assert(0);
            return;
        default:
            length /= 2;
            if (offset < left + length)
            {
                index = index * 2 + 1;
            }
            else
            {
                left += length;
                index = index * 2 + 2;
            }
            break;
        }
    }
}

uint64_t
buddy_size(struct buddy *self, uint64_t offset)
{
    assert(offset < (1lu << self->level));
    uint64_t left = 0;
    uint64_t length = 1lu << self->level;
    uint64_t index = 0;

    for (;;)
    {
        switch (self->tree[index])
        {
        case NODE_USED:
            assert(offset == left);
            return length;
        case NODE_UNUSED:
            assert(0);
            return length;
        default:
            length /= 2;
            if (offset < left + length)
            {
                index = index * 2 + 1;
            }
            else
            {
                left += length;
                index = index * 2 + 2;
            }
            break;
        }
    }
}

static void
_dump(struct buddy *self, uint64_t index, int level)
{
    switch (self->tree[index])
    {
    case NODE_UNUSED:
        printf("(%lu:%lu)", _index_offset(index, level, self->level), 1lu << (self->level - level));
        break;
    case NODE_USED:
        printf("[%lu:%lu]", _index_offset(index, level, self->level), 1lu << (self->level - level));
        break;
    case NODE_FULL:
        printf("{");
        _dump(self, index * 2 + 1, level + 1);
        _dump(self, index * 2 + 2, level + 1);
        printf("}");
        break;
    default:
        printf("(");
        _dump(self, index * 2 + 1, level + 1);
        _dump(self, index * 2 + 2, level + 1);
        printf(")");
        break;
    }
}

void buddy_dump(struct buddy *self)
{
    _dump(self, 0, 0);
    printf("\n");
}
