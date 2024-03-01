#pragma once

#include <vector>
#include <cstdint>
#include <set>

#include "operators.h"
#include "relation.h"
#include "parser.h"
#include "utils.h"

#include <condition_variable>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <thread>

class CheckSum;

class Joiner
{
    friend CheckSum;

private:
    boost::asio::io_service ios_;
    boost::thread_group threadpool_;
    boost::asio::io_service::work work_;

    // 待处理的异步连接数量
    int pendingAsyncJoin_ = 0;
    // 下一个查询的索引
    int nextQueryIdx_ = 0;
    // 存储异步连接结果的向量
    std::vector<std::vector<uint64_t>> asyncResults_;
    // 存储异步连接任务的向量
    std::vector<std::shared_ptr<CheckSum>> asyncJoins_;
    // 用于异步操作的条件变量
    std::condition_variable cond_;
    std::mutex mu_;

    // 线程缓冲区
    char *tBuf_[THREAD_NUM];
    int new_cnt_;

    void newBuf(int idx)
    {
        tBuf_[idx] = (char *)malloc(4 * 2 * 1024 * 1024 * 1024ll);
        __sync_fetch_and_add(&new_cnt_, 1);
    }
    void clearBuf(int idx)
    {
        free(tBuf_[idx]);
    }

public:
    Joiner(int threadNum)
        : work_(ios_)
    {
        asyncResults_.reserve(100);
        asyncJoins_.reserve(100);

        for (int i = 0; i < threadNum; i++)
        {
            threadpool_.create_thread([&]()
                                      {
                tid = __sync_fetch_and_add(&nextTid,1);
                //为tid号线程创建内存缓冲区
                localMemPool[tid] = new MemoryPool(4*1024*1024lu,4096);
                //启动时间循环,处理所有已经提交的异步操作，直到没有待处理事件
                ios_.run(); });
        }
        new_cnt_ = 0;
        for (int i = 0; i < threadNum; i++)
        {
            // 指定线程池任务，异步执行
            ios_.post(std::bind(&Joiner::newBuf, this, i));
        }
        while (new_cnt_ < threadNum)
        {
            // 保证下一次循环前，所有线程已经读取到了new_cnt_
            __sync_synchronize();
        }
        for (int i = 0; i < threadNum; i++)
        {
            ios_.post(std::bind(&Joiner::clearBuf, this, i));
        }
    }
    /// The relations that might be joined
    static std::vector<Relation> relations_;
    /// Add relation
    void addRelation(const char *file_name);
    void addRelation(Relation &&relation);
    /// Get relation
    const Relation &getRelation(unsigned relation_id);
    /// Joins a given set of relations
    void join(QueryInfo &i, int queryIdx);

    // const std::vector<Relation> &relations() const
    // {
    //     return relations_;
    // }
    void waitAsyncJoins();
    std::vector<std::string> getAsyncJoinResults();
    void createAsyncQueryTask(std::string line);

    void printAsyncJoinInfo();
    void loadStat();

    ~Joiner();

private:
    /// Add scan to query
    std::shared_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                      const SelectInfo &info,
                                      QueryInfo &query);
};
