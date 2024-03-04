#pragma once
#include <vector>
#include <cstdint>
#include <set>
#include "Operators.h"
#include "Relation.h"
#include "Parser.h"
#include "Utils.h"
#include "monsoon.h"
#include <condition_variable>
#include "ThreadPool.h"
#include <thread>
#include <atomic>

class Operator;

class Joiner
{
private:
    friend Checksum;
    /// Add scan to query
    std::shared_ptr<Operator> addScan(std::set<unsigned> &usedRelations, SelectInfo &info, QueryInfo &query);
    ThreadPool *threadPool;

    monsoon::IOManager * ioms;
    int pendingAsyncJoin = 0;
    int nextQueryIndex = 0;
    std::vector<std::vector<uint64_t>> asyncResults; // checksums
    std::vector<std::shared_ptr<Checksum>> asyncJoins;
    //    std::vector<std::shared_ptr<Checksum>> tmp;
    std::condition_variable cvAsync;
    std::mutex cvAsyncMt;

    char *buf[THREAD_NUM];
    int cntTouch;

private:
    void touchBuf(int i)
    {
        buf[i] = (char *)malloc(4 * 2 * 1024 * 1024 * 1024ll);
        __sync_fetch_and_add(&cntTouch, 1);
    }
    void clearBuf(int i)
    {
        free(buf[i]);
    }

public:
    Joiner(int threadNum)
    //: work(ioService)
    {
        threadPool = new ThreadPool(threadNum);
        ioms = new monsoon::IOManager(THREAD_NUM,true);

        asyncResults.reserve(100);
        asyncJoins.reserve(100);
        localMemPool = new MemoryPool *[THREAD_NUM];

        for (int i = 0; i < threadNum; i++)
        {
            threadPool->addTask([&]()
                                {
                                    tid = __sync_fetch_and_add(&nextTid, 1);
                                    localMemPool[tid] = new MemoryPool(4 * 1024 * 1024 * 1024lu, 4096);
                                    //ioms[i] = new monsoon::IOManager();
                                    // ioService.run();
                                });
        }

        cntTouch = 0;
        for (int i = 0; i < threadNum; i++)
        {
            // ioService.post(bind(&Joiner::touchBuf, this, i));
            ioms->scheduler(bind(&Joiner::touchBuf, this, i));
        }
        while (cntTouch < threadNum)
        {
            __sync_synchronize();
        }
        for (int i = 0; i < threadNum; i++)
        {
            // ioService.post(bind(&Joiner::clearBuf, this, i));
            ioms->scheduler(bind(&Joiner::clearBuf, this, i));
        }
    }
    /// The relations that might be joined
    static std::vector<Relation> relations;
    /// Add relation
    void addRelation(const char *fileName);
    /// Get relation
    Relation &getRelation(unsigned id);
    /// Joins a given set of relations
    void join(QueryInfo &i, int queryIndex);
    /// wait for async joins
    void waitAsyncJoins();
    /// return parsed asyncResults
    std::vector<std::string> getAsyncJoinResults();
    /// print asyncJoin infos
    void printAsyncJoinInfo();
    void loadStat();

    void createAsyncQueryTask(std::string line);
    ~Joiner()
    {
        // ioService.stop();
        threadPool->~ThreadPool();
        for (int i = 0; i < THREAD_NUM; i++)
        {
            delete localMemPool[i];
        }
        delete[] localMemPool;
    }
};
