#pragma once

#include <vector>
#include <cstdint>
#include <set>

#include "operators.h"
#include "relation.h"
#include "parser.h"

#include <condition_variable>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <thread>

class Joiner
{
friend CheckSum;
private:
    /// The relations that might be joined
    std::vector<Relation> relations_;

    boost::asio::io_service ios_;
    boost::thread_group threadpool_;
    boost::asio::io_service::work work_;

    // 待处理的异步连接数量
    int pendingAsyncJoin_ = 0;
    // 下一个查询的索引
    int nextQueryIdx = 0;
    // 存储异步连接结果的向量
    std::vector<std::vector<uint64_t>> asyncResults_;
    //存储异步连接任务的向量
    std::vector<std::shared_ptr<CheckSum>>asyncJoins;
    //用于异步操作的条件变量
    std::condition_variable cond_;
    std::mutex mu_;

    

public:
    /// Add relation
    void addRelation(const char *file_name);
    void addRelation(Relation &&relation);
    /// Get relation
    const Relation &getRelation(unsigned relation_id);
    /// Joins a given set of relations
    std::string join(QueryInfo &i);

    const std::vector<Relation> &relations() const
    {
        return relations_;
    }

private:
    /// Add scan to query
    std::unique_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                      const SelectInfo &info,
                                      QueryInfo &query);
};
