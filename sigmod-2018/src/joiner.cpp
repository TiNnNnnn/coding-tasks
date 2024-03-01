#include "joiner.h"

#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include <sstream>
#include <assert.h>

#include "utils.h"
#include "parser.h"
#include "operators.h"

namespace
{

    enum QueryGraphProvides
    {
        Left,
        Right,
        Both,
        None
    };

    // Analyzes inputs of join
    QueryGraphProvides analyzeInputOfJoin(std::set<unsigned> &usedRelations,
                                          SelectInfo &leftInfo,
                                          SelectInfo &rightInfo)
    {
        bool used_left = usedRelations.count(leftInfo.binding);
        bool used_right = usedRelations.count(rightInfo.binding);

        if (used_left ^ used_right)
            return used_left ? QueryGraphProvides::Left : QueryGraphProvides::Right;
        if (used_left && used_right)
            return QueryGraphProvides::Both;
        return QueryGraphProvides::None;
    }

}

std::vector<Relation> Joiner::relations_;

// Loads a relation_ from disk
void Joiner::addRelation(const char *file_name)
{
    relations_.emplace_back(file_name);
}

void Joiner::addRelation(Relation &&relation)
{
    relations_.emplace_back(std::move(relation));
}

// Loads a relation from disk
const Relation &Joiner::getRelation(unsigned relation_id)
{
    if (relation_id >= relations_.size())
    {
        std::cerr << "Relation with id: " << relation_id << " does not exist"
                  << std::endl;
        throw;
    }
    return relations_[relation_id];
}

// 将扫描操作添加到查询中
std::shared_ptr<Operator> Joiner::addScan(std::set<unsigned> &used_relations,
                                          const SelectInfo &info,
                                          QueryInfo &query)
{
    used_relations.emplace(info.binding);
    std::vector<FilterInfo> filters;
    for (auto &f : query.filters())
    {
        if (f.filter_column.binding == info.binding)
        {
            filters.emplace_back(f);
        }
    }
    return !filters.empty() ? std::make_shared<FilterScan>(getRelation(info.rel_id), filters)
                            : std::make_shared<Scan>(getRelation(info.rel_id),
                                                     info.binding);
}

void Joiner::waitAsyncJoins()
{
    std::unique_lock<std::mutex> lock(mu_);
    if (pendingAsyncJoin_ > 0)
    {
        std::cout << "Joiner::waitAsynceJoins wait" << std::endl;
        // 还有异步任务没有处理完，阻塞当前
        cond_.wait(lock);
        std::cout << "Joiner::waitAsynceJoins wakeup" << std::endl;
    }
}

std::vector<std::string> Joiner::getAsyncJoinResults()
{
    std::vector<std::string> res;
    std::stringstream out;
    for (auto &queryRes : asyncResults_)
    {
        for (unsigned i = 0; i < queryRes.size(); i++)
        {
            out << (queryRes[i] == 0 ? "NULL" : std::to_string(queryRes[i]));
            if (i < queryRes.size() - 1)
            {
                out << " ";
            }
        }
        out << "\n";
        res.push_back(out.str());
        out.str("");
    }
    asyncResults_.clear();
    asyncJoins_.clear();
    nextQueryIdx_ = 0;
    return res;
}

void Joiner::createAsyncQueryTask(std::string line)
{
    __sync_fetch_and_add(&pendingAsyncJoin_, 1);
    // 解析查询字符串
    QueryInfo query;
    query.parseQuery(line);
    // 提前添加空向量
    asyncJoins_.emplace_back();
    asyncResults_.emplace_back();
    // 提交查询任务
    ios_.post(std::bind(&Joiner::join, this, query, nextQueryIdx_));
    __sync_fetch_and_add(&nextQueryIdx_, 1);
}

// 执行join操作
void Joiner::join(QueryInfo &query, int queryIdx)
{
    std::set<unsigned> used_relations;

    // 总是选择从第一个连接条件开始，将其他连接追加到它上面
    const auto &firstJoin = query.predicates()[0];
    std::shared_ptr<Operator> left, right;
    // 添加左扫描
    left = addScan(used_relations, firstJoin.left, query);
    // 添加右扫描
    right = addScan(used_relations, firstJoin.right, query);
    // 创建连接操作并设置为根节点
    std::shared_ptr<Operator>
        root = std::make_shared<Join>(left, right, firstJoin);

    left->setParent(root);
    right->setParent(root);

    auto predicates_copy = query.predicates();
    for (unsigned i = 1; i < predicates_copy.size(); ++i)
    {
        auto &p_info = predicates_copy[i];
        Utils::CondPanic(p_info.left < p_info.right, "join left >= right");
        auto &left_info = p_info.left;
        auto &right_info = p_info.right;

        switch (analyzeInputOfJoin(used_relations, left_info, right_info))
        {
        case QueryGraphProvides::Left:
            left = root;
            right = addScan(used_relations, right_info, query);
            root = std::make_shared<Join>(left, right, p_info);
            left->setParent(root);
            right->setParent(root);
            break;
        case QueryGraphProvides::Right:
            left = addScan(used_relations, left_info, query);
            right = root;
            root = std::make_shared<Join>(left, right, p_info);
            left->setParent(root);
            right->setParent(root);
            break;
        case QueryGraphProvides::Both:
            // All relations of this join are already used somewhere else in the
            // query. Thus, we have either a cycle in our join graph or more than
            // one join predicate per join.
            left = root;
            root = std::make_shared<SelfJoin>(root, p_info);
            break;
        case QueryGraphProvides::None:
            // Process this predicate later when we can connect it to the other
            // joins. We never have cross products.
            predicates_copy.push_back(p_info);
            break;
        };
    }

    std::shared_ptr<CheckSum> checksum = std::make_shared<CheckSum>(*this, root, query.selections());
    // Checksum checksum(*this,root, query.selections());
    root->setParent(checksum);
    asyncJoins_[queryIdx] = checksum;
    checksum->asynRun(ios_, queryIdx);

    // checksum.run();

    // std::stringstream out;
    // auto &results = checksum.check_sums();
    // for (unsigned i = 0; i < results.size(); ++i)
    // {
    //     out << (checksum.result_size() == 0 ? "NULL" : std::to_string(results[i]));
    //     if (i < results.size() - 1)
    //         out << " ";
    // }
    // out << "\n";
    // return out.str();
}

void Joiner::loadStat()
{
    for (auto &r : relations_)
    {
        for (unsigned i = 0; i < r.columns_.size(); i++)
        {
            ios_.post(std::bind(&Relation::loadStat, &r, i));
        }
    }
    // 等待所有统计信息加载完成
    while (1)
    {
        bool done = true;
        for (auto &r : relations_)
        {
            for (unsigned i = 0; i < r.columns_.size(); i++)
            {
                if (r.needCount_[i] == -1)
                {
                    done = false;
                    break;
                }
            }
            if (done)
            {
                break;
            }
        }
        usleep(100000);
    }
}

Joiner::~Joiner()
{
    ios_.stop();
    threadpool_.join_all();
    for (int i = 0; i < THREAD_NUM; i++)
    {
        delete localMemPool[i];
    }
    delete[] localMemPool;
}
