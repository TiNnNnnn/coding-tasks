#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>

#include "Utils.h"

using namespace std;
MemoryPool **localMemPool;
thread_local int tid = 0;
int nextTid = 0;
unsigned cnt = 0;

static void createColumn(vector<uint64_t *> &columns, uint64_t numTuples)
// Create a dummy column
{
    auto col = new uint64_t[numTuples];
    columns.push_back(col);
    for (unsigned i = 0; i < numTuples; ++i)
    {
        col[i] = i;
    }
}

// Create a dummy relation
Relation Utils::createRelation(uint64_t size, uint64_t numColumns)
{
    vector<uint64_t *> columns;
    for (unsigned i = 0; i < numColumns; ++i)
    {
        createColumn(columns, size);
    }
    return Relation(size, move(columns));
}

void Utils::storeRelation(ofstream &out, Relation &r, unsigned i)
// Store a relation in all formats
{
    auto baseName = "r" + to_string(i);
    r.storeRelation(baseName);
    r.storeRelationCSV(baseName);
    r.dumpSQL(baseName, i);
    cout << baseName << "\n";
    out << baseName << "\n";
}

int Utils::log2(unsigned v)
{
    int n = -1;
    while (v != 0)
    {
        v >>= 1;
        n++;
    }
    return n;
}

std::thread::id Utils::GetThreadId()
{
    return std::this_thread::get_id();
}
