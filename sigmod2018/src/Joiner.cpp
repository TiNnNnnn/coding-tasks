#include "Joiner.h"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include <atomic>
#include "Parser.h"

using namespace std;

std::vector<Relation> Joiner::relations;
void Joiner::addRelation(const char *fileName)
// Loads a relation from disk
{
    relations.emplace_back(fileName);
}

void Joiner::printAsyncJoinInfo()
{
    __sync_synchronize();
    for (int i = 0; i < asyncJoins.size(); i++)
    {
        cout << "-----------------Query " << i << "---------------" << endl;
        if (asyncJoins[i] != nullptr && asyncJoins[i]->pendingAsyncOperator != 0)
            asyncJoins[i]->printAsyncInfo();
    }
}

void Joiner::waitAsyncJoins()
{
    unique_lock<mutex> lk(cvAsyncMt);
    if (pendingAsyncJoin > 0)
    {
        cvAsync.wait(lk);
    }
}

vector<string> Joiner::getAsyncJoinResults()
{
    vector<string> results;

    stringstream out;
    for (auto &queryResult : asyncResults)
    {
        for (unsigned i = 0; i < queryResult.size(); ++i)
        {
            out << (queryResult[i] == 0 ? "NULL" : to_string(queryResult[i]));
            if (i < queryResult.size() - 1)
                out << " ";
        }
        out << "\n";
        results.push_back(out.str());
        out.str("");
    }

    asyncResults.clear();
    //    tmp.insert(tmp.end(), asyncJoins.begin(), asyncJoins.end());
    // ioService.post(bind([](vector<shared_ptr<Checksum>> ops) {}, asyncJoins)); // gc, asynchronous discount shared pointer and release
    asyncJoins.clear();
    nextQueryIndex = 0;

    return results;
}
//---------------------------------------------------------------------------
Relation &Joiner::getRelation(unsigned relationId)
// Loads a relation from disk
{
    if (relationId >= relations.size())
    {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}
//---------------------------------------------------------------------------
shared_ptr<Operator> Joiner::addScan(set<unsigned> &usedRelations, SelectInfo &info, QueryInfo &query)
// Add scan to query
{
    usedRelations.emplace(info.binding);
    vector<FilterInfo> filters;
    for (auto &f : query.filters)
    {
        if (f.filterColumn.binding == info.binding)
        {
            filters.emplace_back(f);
        }
    }
    return filters.size() ? make_shared<FilterScan>(getRelation(info.relId), filters) : make_shared<Scan>(getRelation(info.relId), info.binding);
}
//---------------------------------------------------------------------------
enum QueryGraphProvides
{
    Left,
    Right,
    Both,
    None
};
//---------------------------------------------------------------------------
static QueryGraphProvides analyzeInputOfJoin(set<unsigned> &usedRelations, SelectInfo &leftInfo, SelectInfo &rightInfo)
// Analyzes inputs of join
{
    bool usedLeft = usedRelations.count(leftInfo.binding);
    bool usedRight = usedRelations.count(rightInfo.binding);

    if (usedLeft ^ usedRight)
        return usedLeft ? QueryGraphProvides::Left : QueryGraphProvides::Right;
    if (usedLeft && usedRight)
        return QueryGraphProvides::Both;
    return QueryGraphProvides::None;
}

// 构建执行树，生成执行计划
void Joiner::join(QueryInfo &query, int queryIndex)
// Executes a join query
{
    //cerr << query.dumpText() << endl;
    set<unsigned> usedRelations;
    // We always start with the first join predicate and append the other joins to it (--> left-deep join trees)
    // You might want to choose a smarter join ordering ...
    auto &firstJoin = query.predicates[0];

    // for (int i = 0; i < query.predicates.size(); i++)
    // {
    //     cerr << "{";
    //     cerr << query.predicates[i].left.relId << " ";
    //     cerr << query.predicates[i].left.binding << " ";
    //     cerr << query.predicates[i].left.colId << " |";
    //     cerr << query.predicates[i].right.relId << " ";
    //     cerr << query.predicates[i].right.binding << " ";
    //     cerr << query.predicates[i].right.colId << " ";
    //     cerr << "}";
    // }
    // cerr << endl;

    auto left = addScan(usedRelations, firstJoin.left, query);
    auto right = addScan(usedRelations, firstJoin.right, query);

    shared_ptr<Operator> root = make_shared<Join>(left, right, firstJoin);
    left->setParent(root);
    right->setParent(root);

    //cerr << "query size: " << query.predicates.size() << endl;

    for (unsigned i = 1; i < query.predicates.size(); ++i)
    {
        auto &pInfo = query.predicates[i];
        assert(pInfo.left < pInfo.right);
        auto &leftInfo = pInfo.left;
        auto &rightInfo = pInfo.right;
        shared_ptr<Operator> left, right;
        switch (analyzeInputOfJoin(usedRelations, leftInfo, rightInfo))
        {
        case QueryGraphProvides::Left:
            //cerr << "Left" << endl;
            left = root;
            right = addScan(usedRelations, rightInfo, query);
            root = make_shared<Join>(left, right, pInfo);
            left->setParent(root);
            right->setParent(root);
            break;
        case QueryGraphProvides::Right:
            //cerr << "Right" << endl;
            left = addScan(usedRelations, leftInfo, query);
            right = root;
            root = make_shared<Join>(left, right, pInfo);
            left->setParent(root);
            right->setParent(root);
            break;
        case QueryGraphProvides::Both:
            //cerr << "Both" << endl;
            // All relations of this join are already used somewhere else in the query.
            // Thus, we have either a cycle in our join graph or more than one join predicate per join.
            left = root;
            root = make_shared<SelfJoin>(left, pInfo);
            left->setParent(root);
            break;
        case QueryGraphProvides::None:
            //cerr << "None" << endl;
            // Process this predicate later when we can connect it to the other joins
            // We never have cross products
            query.predicates.push_back(pInfo);
            break;
        };
    }

    // 生成验证计划，并标记为根节点
    std::shared_ptr<Checksum> checkSum = std::make_shared<Checksum>(*this, root, query.selections);
    root->setParent(checkSum);
    asyncJoins[queryIndex] = checkSum;
    // 执行计划
    checkSum->asyncRun(*ioms, queryIndex);
}
//---------------------------------------------------------------------------
void Joiner::createAsyncQueryTask(string line)
{
    __sync_fetch_and_add(&pendingAsyncJoin, 1);
    QueryInfo query;
    query.parseQuery(line);
    asyncJoins.emplace_back();
    asyncResults.emplace_back();

    ioms->scheduler(bind(&Joiner::join, this, query, nextQueryIndex));
    __sync_fetch_and_add(&nextQueryIndex, 1);
}
//---------------------------------------------------------------------------
void Joiner::loadStat()
{
    for (auto &r : relations)
    {
        for (unsigned i = 0; i < r.columns.size(); ++i)
        {
            ioms->scheduler(bind(&Relation::loadStat, &r, i));
        }
    }
    while (1)
    {
        bool done = true;
        for (auto &r : relations)
        {
            for (unsigned i = 0; i < r.columns.size(); ++i)
            {
                if (r.needCount[i] == -1)
                {
                    done = false;
                    break;
                }
            }
            if (!done)
            {
                break;
            }
        }
        if (done)
        {
            break;
        }
        usleep(100000);
    }
}
