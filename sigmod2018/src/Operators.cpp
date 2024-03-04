#include "Operators.h"
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>
#include "Joiner.h"
#include "Config.h"
#include "Utils.h"
#include "monsoon.h"
#include <sys/mman.h>

using namespace std;

// Get materialized results
vector<Column<uint64_t>> &Operator::getResults()
{
    return results;
}

uint64_t Operator::getResultsSize()
{
    return resultSize * results.size() * 8;
}

void Operator::finishAsyncRun(monsoon::IOManager &ioService, bool startParentAsync)
{
    isStopped = true;
    // 左右任务已经完成，开始执行父任务
    if (auto p = parent.lock())
    {
        if (resultSize == 0)
            p->stop();
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
        assert(pending >= 0);
        if (pending == 0 && startParentAsync)
            p->createAsyncTasks(ioService);
    }
    else
    {
        // 已经是root节点
    }
}

// Require a column and add it to results
bool Scan::require(SelectInfo info)
{
    // cerr << "Scan::require" << endl;
    if (info.binding != relationBinding)
    {
        return false;
    }
    assert(info.colId < relation.columns.size());
    if (select2ResultColId.find(info) == select2ResultColId.end())
    {
        // 提前开辟结果空间
        results.emplace_back(1);
        // 添加待处理列
        infos.push_back(info);
        // 标记为已添加列
        select2ResultColId[info] = results.size() - 1;
    }
    return true;
}

uint64_t Scan::getResultsSize()
{
    return results.size() * relation.size * 8;
}

void Scan::asyncRun(monsoon::IOManager &ioService)
{
    //cerr << "Scan::run" << endl;
    pendingAsyncOperator = 0;
    // 如果只有一列且之前已经进行了元素统计
    if (infos.size() == 1 && !relation.counted[infos[0].colId].empty())
    {
        // resultSize为当前列不同元素个数，将当前列和计数列加入运行result中
        resultSize = relation.counted[infos[0].colId][1] - relation.counted[infos[0].colId][0];
        results[0].addTuples(0, relation.counted[infos[0].colId][0], resultSize); // Count Column
        results[0].fix();

        results.emplace_back(1);
        results[1].addTuples(0, relation.counted[infos[0].colId][1], resultSize); // Count Column
        results[1].fix();
        counted = 2;
    }
    else
    {
        // 扫描所有需要处理的列
        for (int i = 0; i < infos.size(); i++)
        {
            results[i].addTuples(0, relation.columns[infos[i].colId], relation.size);
            results[i].fix();
        }
        // resultSize为总行数
        resultSize = relation.size;
    }
    finishAsyncRun(ioService, true);
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info)
{
    if (info.binding != relationBinding)
        return false;
    assert(info.colId < relation.columns.size());
    if (select2ResultColId.find(info) == select2ResultColId.end())
    {
        infos.push_back(info);
        unsigned colId = infos.size() - 1;
        select2ResultColId[info] = colId;
    }
    return true;
}

// Apply filter
bool FilterScan::applyFilter(uint64_t i, FilterInfo &f)
{
    auto compareCol(counted == 2 ? relation.counted[f.filterColumn.colId][0] : relation.columns[f.filterColumn.colId]);
    auto constant = f.constant;
    switch (f.comparison)
    {
    case FilterInfo::Comparison::Equal:
        return compareCol[i] == constant;
    case FilterInfo::Comparison::Greater:
        return compareCol[i] > constant;
    case FilterInfo::Comparison::Less:
        return compareCol[i] < constant;
    };
    return false;
}

void FilterScan::asyncRun(monsoon::IOManager &ioService)
{
    //cerr << "FilterScan::run" << endl;
    pendingAsyncOperator = 0;
    __sync_synchronize();
    createAsyncTasks(ioService);
}

void FilterScan::createAsyncTasks(monsoon::IOManager &ioService)
{
    __sync_synchronize();
    if (isStopped)
    {

        finishAsyncRun(ioService, true);
        return;
    }
    int cntTask = THREAD_NUM;
    // 当前表的行数
    uint64_t size = relation.size;
    // 待筛选的列数为1
    if (infos.size() == 1)
    {
        bool pass = true;
        for (auto &f : filters)
        {
            if (f.filterColumn.colId != infos[0].colId)
            {
                pass = false;
                break;
            }
        }
        // 之前已经预统计了该列每个元素的频次
        if (pass && !relation.counted[infos[0].colId].empty())
        {
            counted = 2;
            // 行数等于表中当前列不同元素的个数
            size = relation.counted[infos[0].colId][1] - relation.counted[infos[0].colId][0];
        }
        if (!counted && relation.needCount[infos[0].colId])
        {
            counted = 1;
        }
    }
    // 每个线程平均扫描行数
    uint64_t taskLength = size / cntTask;
    uint64_t rest = size % cntTask;

    // 平均任务负载过低，重写调整
    if (taskLength < minTuplesPerTask)
    {
        cntTask = size / minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = size / cntTask;
        rest = size % cntTask;
    }
    // 待执行任务数量
    pendingTask = cntTask;

    if (counted == 2)
    {
        // 如果之前已经对元素频率进行了统计
        inputData.emplace_back(relation.counted[infos[0].colId][0]);
        inputData.emplace_back(relation.counted[infos[0].colId][1]);
        results.emplace_back(cntTask);
        results.emplace_back(cntTask);
    }
    else
    {
        for (auto &sInfo : infos)
        {
            inputData.emplace_back(relation.columns[sInfo.colId]);
            results.emplace_back(cntTask);
        }
        if (counted)
        {
            results.emplace_back(cntTask);
        }

        // for (int i = 0; i < inputData.size(); i++)
        // {
        //     cerr << "fiterscan inputdata: " << *inputData[i] << endl;
        // }
    }

    for (int i = 0; i < cntTask; i++)
    {
        tmpResults.emplace_back();
    }

    __sync_synchronize();

    // 给线程分配任务
    uint64_t start = 0;
    for (unsigned i = 0; i < cntTask; i++)
    {
        uint64_t length = taskLength;
        if (rest)
        {
            length++;
            rest--;
        }
        ioService.scheduler(bind(&FilterScan::filterTask, this, &ioService, i, start, length));
        start += length;
    }
}

void FilterScan::filterTask(monsoon::IOManager *ioService, int taskIndex, uint64_t start, uint64_t length)
{
    // atomic_cout << "[tid: " << Utils::GetThreadId() << "] run filtertask " << endl;
    //  當前為空
    vector<vector<uint64_t>> &localResults = tmpResults[taskIndex];
    unsigned colSize = inputData.size();
    unordered_map<uint64_t, unsigned> cntMap;

    __sync_synchronize();
    if (isStopped)
    {
        goto fs_finish;
    }

    for (int j = 0; j < inputData.size(); j++)
    {
        localResults.emplace_back();
    }
    // 增加计数列
    if (counted)
    {
        localResults.emplace_back();
    }

    for (uint64_t i = start; i < start + length; ++i)
    {
        bool pass = true;
        for (auto &f : filters)
        {
            // 应用过滤器
            if (!(pass = applyFilter(i, f)))
                break;
        }
        if (pass)
        {
            if (counted == 1)
            {
                // 加入count column
                auto iter = cntMap.find(inputData[0][i]);
                if (iter != cntMap.end())
                {
                    ++localResults[1][iter->second];
                }
                else
                {
                    localResults[0].push_back(inputData[0][i]);
                    localResults[1].push_back(1);
                    cntMap.insert(iter, pair<uint64_t, unsigned>(inputData[0][i], localResults[1].size() - 1));
                }
            }
            else
            {
                // count == 2, colSize already contains count column
                for (unsigned cId = 0; cId < colSize; ++cId)
                    localResults[cId].push_back(inputData[cId][i]);
            }
        }
    }

    for (unsigned cId = 0; cId < colSize; ++cId)
    {
        results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
    }
    if (counted == 1)
    { // Use calculated count column
        results[1].addTuples(taskIndex, localResults[1].data(), localResults[1].size());
    }

    // resultSize += localResults[0].size();
    __sync_fetch_and_add(&resultSize, localResults[0].size());

fs_finish:
    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0)
    {
        for (unsigned cId = 0; cId < colSize; ++cId)
        {
            results[cId].fix();
        }
        if (counted == 1)
        {
            results[1].fix();
        }
        finishAsyncRun(*ioService, true);
    }
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo info)
// Require a column and add it to results
{
    // cerr << "Join::require" << endl;
    if (requestedColumns.count(info) == 0)
    {
        bool success = false;
        if (left->require(info))
        {
            requestedColumnsLeft.emplace_back(info);
            success = true;
        }
        else if (right->require(info))
        {
            success = true;
            requestedColumnsRight.emplace_back(info);
        }
        if (!success)
            return false;

        requestedColumns.emplace(info);
    }
    return true;
}
//---------------------------------------------------------------------------
void Join::asyncRun(monsoon::IOManager &ioService)
{
    //cerr << "Join::run" << endl;
    pendingAsyncOperator = 2;
    // 请求左右节点的待处理列
    left->require(pInfo.left);
    right->require(pInfo.right);
    __sync_synchronize();
    // 分别执行左右节点
    //cerr << "Join left-->";
    left->asyncRun(ioService);
    //cerr << "Join right-->";
    right->asyncRun(ioService);
}

void Join::createAsyncTasks(monsoon::IOManager &ioService)
{
    assert(pendingAsyncOperator == 0);
    __sync_synchronize();
    if (isStopped)
    {
        finishAsyncRun(ioService, true);
        return;
    }
    // join时小表在左边
    if (left->resultSize > right->resultSize)
    {
        swap(left, right);
        swap(pInfo.left, pInfo.right);
        swap(requestedColumnsLeft, requestedColumnsRight);
    }

    auto &leftInputData = left->getResults();
    auto &rightInputData = right->getResults();

    unsigned resColId = 0;
    for (auto &info : requestedColumnsLeft)
    {
        select2ResultColId[info] = resColId++;
    }
    for (auto &info : requestedColumnsRight)
    {
        select2ResultColId[info] = resColId++;
    }

    if (requestedColumnsLeft.size() == 0 ||
        (requestedColumnsLeft.size() == 1 && requestedColumnsLeft[0] == pInfo.left))
    {
        cntBuilding = true;
    }
    // no reuslts
    if (left->resultSize == 0)
    {
        finishAsyncRun(ioService, true);
        return;
    }

    if (requestedColumnsLeft.size() + requestedColumnsRight.size() == 1)
    {
        auto &sInfo(requestedColumnsLeft.size() == 1 ? requestedColumnsLeft[0] : requestedColumnsRight[0]);
        counted = Joiner::relations[sInfo.relId].needCount[sInfo.colId];
    }
    if ((left->counted || right->counted || cntBuilding) && !counted)
    {
        counted = 2;
    }

    leftColId = left->resolve(pInfo.left);
    rightColId = right->resolve(pInfo.right);

    // 计算HASH分区数量，向上取整，得到2的整数次幂
    cntPartition = CNT_PARTITIONS(left->resultSize * 8 * 2, partitionSize); // uint64*2(key, value)
    if (cntPartition < 32)
        cntPartition = 32;
    cntPartition = 1 << (Utils::log2(cntPartition - 1) + 1);
    pendingPartitioning = 2;

    // 分配内存用于存储分区表
    partitionTable[0] = (uint64_t *)localMemPool[tid]->alloc(left->getResultsSize());
    partitionTable[1] = (uint64_t *)localMemPool[tid]->alloc(right->getResultsSize());
    allocTid = tid;

    // 初始化分区、直方图和任务相关的数据结构
    for (uint64_t i = 0; i < cntPartition; i++)
    {
        partition[0].emplace_back();
        partition[1].emplace_back();
        for (unsigned j = 0; j < leftInputData.size(); j++)
        {
            partition[0][i].emplace_back();
        }
        for (unsigned j = 0; j < rightInputData.size(); j++)
        {
            partition[1][i].emplace_back();
        }
        tmpResults.emplace_back();
    }

    int cntTaskLeft = THREAD_NUM;
    int cntTaskRight = THREAD_NUM;
    taskLength[0] = left->resultSize / cntTaskLeft;
    taskLength[1] = right->resultSize / cntTaskRight;
    taskRest[0] = left->resultSize % cntTaskLeft;
    taskRest[1] = right->resultSize % cntTaskRight;

    // 调整任务长度
    if (taskLength[0] < minTuplesPerTask)
    {
        cntTaskLeft = left->resultSize / minTuplesPerTask;
        if (cntTaskLeft == 0)
            cntTaskLeft = 1;
        taskLength[0] = left->resultSize / cntTaskLeft;
        taskRest[0] = left->resultSize % cntTaskLeft;
    }
    if (taskLength[1] < minTuplesPerTask)
    {
        cntTaskRight = right->resultSize / minTuplesPerTask;
        if (cntTaskRight == 0)
            cntTaskRight = 1;
        taskLength[1] = right->resultSize / cntTaskRight;
        taskRest[1] = right->resultSize % cntTaskRight;
    }
    // 为直方图预留空间
    histograms[0].reserve(cntTaskLeft);
    histograms[1].reserve(cntTaskRight);
    for (int i = 0; i < cntTaskLeft; i++)
    {
        histograms[0].emplace_back();
    }
    for (int i = 0; i < cntTaskRight; i++)
    {
        histograms[1].emplace_back();
    }
    pendingMakingHistogram[0] = cntTaskLeft;
    pendingMakingHistogram[1 * CACHE_LINE_SIZE] = cntTaskRight;

    __sync_synchronize();

    uint64_t startLeft = 0;
    uint64_t restLeft = taskRest[0];
    for (int i = 0; i < cntTaskLeft; i++)
    {
        uint64_t lengthLeft = taskLength[0];
        if (restLeft)
        {
            lengthLeft++;
            restLeft--;
        }
        ioService.scheduler(bind(&Join::histogramTask, this, &ioService, cntTaskLeft, i, 0, startLeft, lengthLeft)); // for left
        startLeft += lengthLeft;
    }

    uint64_t startRight = 0;
    uint64_t restRight = taskRest[1];
    for (int i = 0; i < cntTaskRight; i++)
    {
        uint64_t lengthRight = taskLength[1];
        if (restRight)
        {
            lengthRight++;
            restRight--;
        }
        // 执行直方图统计任务，了解数据分布
        ioService.scheduler(bind(&Join::histogramTask, this, &ioService, cntTaskRight, i, 1, startRight, lengthRight)); // for right
        startRight += lengthRight;
    }
}

//---------------------------------------------------------------------------
void Join::histogramTask(monsoon::IOManager *ioService, int cntTask, int taskIndex, int leftOrRight, uint64_t start, uint64_t length)
{
    vector<Column<uint64_t>> &inputData = !leftOrRight ? left->getResults() : right->getResults();
    Column<uint64_t> &keyColumn = !leftOrRight ? inputData[leftColId] : inputData[rightColId];
    histograms[leftOrRight][taskIndex].reserve(CACHE_LINE_SIZE); // for preventing false sharing
    for (uint64_t j = 0; j < cntPartition; j++)
    {
        histograms[leftOrRight][taskIndex].emplace_back();
    }
    auto it = keyColumn.begin(start);
    for (uint64_t i = start, limit = start + length; i < limit; i++, ++it)
    {
        histograms[leftOrRight][taskIndex][RADIX_HASH(*it, cntPartition)]++;
    }
    int remainder = __sync_sub_and_fetch(&pendingMakingHistogram[leftOrRight * CACHE_LINE_SIZE], 1);

    // 所有直方图任务均已完成
    if (UNLIKELY(remainder == 0))
    {
        // 对每个哈希桶进行数据分片
        for (int i = 0; i < cntPartition; i++)
        {
            partitionLength[leftOrRight].push_back(0);
            // 计算每个哈希桶的数据长度，并在必要时进行累加
            for (int j = 0; j < cntTask; j++)
            {
                partitionLength[leftOrRight][i] += histograms[leftOrRight][j][i];
                if (j != 0)
                { // make histogram to containprefix-sum
                    histograms[leftOrRight][j][i] += histograms[leftOrRight][j - 1][i];
                }
            }
        }
        // 如果处理的是右表数据，则准备进行哈希连接的后续操作
        if (leftOrRight == 1)
        {
            resultIndex.push_back(0);
            for (int i = 0; i < cntPartition; i++)
            {
                // 计算每个哈希桶的数据长度，并准备用于哈希连接操作
                uint64_t limitRight = partitionLength[1][i];
                unsigned cntTask = THREAD_NUM;
                uint64_t taskLength = limitRight / cntTask;
                unsigned rest = limitRight % cntTask;

                if (taskLength < minTuplesPerTask)
                {
                    cntTask = limitRight / minTuplesPerTask;
                    if (cntTask == 0)
                        cntTask = 1;
                    taskLength = limitRight / cntTask;
                    rest = limitRight % cntTask;
                }
                cntProbing.push_back(cntTask);
                lengthProbing.push_back(taskLength);
                restProbing.push_back(rest);
                resultIndex.push_back(cntTask);
                resultIndex[i + 1] += resultIndex[i];

                // hashTables.emplace_back();
            }
            // 计算哈希连接的结果集大小，并为结果集分配空间
            unsigned probingResultSize = resultIndex[cntPartition];
            for (int i = 0; i < requestedColumns.size(); i++)
            {
                results.emplace_back(probingResultSize);
            }
            if (counted)
            {
                results.emplace_back(probingResultSize); // For Count Column
            }
        }
        // 为哈希分区表分配空间
        auto cntColumns = inputData.size(); // requestedColumns.size();
        uint64_t *partAddress = partitionTable[leftOrRight];
        // partition[leftOrRight][0][0] = partitionTable[leftOrRight];
        for (uint64_t i = 0; i < cntPartition; i++)
        {
            uint64_t cntTuples = partitionLength[leftOrRight][i]; // histograms[leftOrRight][cntTask-1][i];
            for (unsigned j = 0; j < cntColumns; j++)
            {
                partition[leftOrRight][i][j] = partAddress + j * cntTuples;
            }
            partAddress += cntTuples * cntColumns;
        }
        // 所有直方图任务完成，提交哈希分片任务
        pendingScattering[leftOrRight * CACHE_LINE_SIZE] = cntTask;
        __sync_synchronize();

        uint64_t start = 0;
        uint64_t rest = taskRest[leftOrRight];

        for (int i = 0; i < cntTask; i++)
        {
            uint64_t length = taskLength[leftOrRight];
            if (rest)
            {
                length++;
                rest--;
            }
            ioService->scheduler(bind(&Join::scatteringTask, this, ioService, i, leftOrRight, start, length));
            start += length;
        }
    }
}

void Join::scatteringTask(monsoon::IOManager *ioService, int taskIndex, int leftOrRight, uint64_t start, uint64_t length)
{
    vector<Column<uint64_t>> &inputData = !leftOrRight ? left->getResults() : right->getResults();
    Column<uint64_t> &keyColumn = !leftOrRight ? inputData[leftColId] : inputData[rightColId];
    auto keyIt = keyColumn.begin(start);
    vector<Column<uint64_t>::Iterator> colIt;
    vector<uint64_t> insertOffs;
    __sync_synchronize();
    if (isStopped)
    {
        goto scattering_finish;
    }
    // 为每个哈希桶创建插入偏移数组
    for (int i = 0; i < cntPartition; i++)
    {
        insertOffs.push_back(0);
    }
    // 初始化每列的迭代器
    for (unsigned i = 0; i < inputData.size(); i++)
    {
        colIt.push_back(inputData[i].begin(start));
    }
    // 遍历输入数据，将数据插入到相应的哈希桶中
    for (uint64_t i = start, limit = start + length; i < limit; i++, ++keyIt)
    {
        uint64_t hashResult = RADIX_HASH(*keyIt, cntPartition);
        uint64_t insertBase;
        uint64_t insertOff = insertOffs[hashResult]++;
        // 计算插入基址
        if (UNLIKELY(taskIndex == 0))
            insertBase = 0;
        else
        {
            insertBase = histograms[leftOrRight][taskIndex - 1][hashResult];
        }
        for (unsigned j = 0; j < inputData.size(); j++)
        {
            partition[leftOrRight][hashResult][j][insertBase + insertOff] = *(colIt[j]);
            ++(colIt[j]);
        }
    }

scattering_finish:
    int remainder = __sync_sub_and_fetch(&pendingScattering[leftOrRight * CACHE_LINE_SIZE], 1);
    // 所有哈希分片任务均已完成
    if (UNLIKELY(remainder == 0))
    {
        int remPart = __sync_sub_and_fetch(&pendingPartitioning, 1);
        if (remPart == 0)
        {
            if (isStopped)
            {
                finishAsyncRun(*ioService, true);
                return;
            }
            // 选择需要进行哈希连接的子任务
            vector<unsigned> subJoinTarget;
            for (int i = 0; i < cntPartition; i++)
            {
                if (partitionLength[0][i] != 0 && partitionLength[1][i] != 0)
                {
                    subJoinTarget.push_back(i);
                }
            }
            // 如果没有需要连接的子任务，则完成异步运行
            if (subJoinTarget.size() == 0)
            {
                for (unsigned cId = 0; cId < requestedColumns.size(); ++cId)
                {
                    results[cId].fix();
                }
                if (counted)
                {
                    results[requestedColumns.size()].fix(); // For Count Column
                }
                localMemPool[allocTid]->requestFree(partitionTable[0]);
                localMemPool[allocTid]->requestFree(partitionTable[1]);
                finishAsyncRun(*ioService, true);
                return;
            }
            // 根据需要连接的子任务数量，分配哈希表内存
            for (int i = 0; i < cntPartition; i++)
            {
                if (cntBuilding)
                    hashTablesCnt.emplace_back();
                else
                    hashTablesIndices.emplace_back();
            }
            // 初始化哈希表指针数组
            for (int i = 0; i < cntPartition; i++)
            {
                if (cntBuilding)
                    hashTablesCnt[i] = NULL;
                else
                    hashTablesIndices[i] = NULL;
            }
            // 设置待构建哈希表的数量，并同步
            pendingBuilding = subJoinTarget.size();
            unsigned taskNum = pendingBuilding;
            __sync_synchronize();
            for (auto &s : subJoinTarget)
            {
                ioService->scheduler(bind(&Join::buildingTask, this, ioService, s, partition[0][s], partitionLength[0][s], partition[1][s], partitionLength[1][s]));
            }
        }
    }
}

void Join::buildingTask(monsoon::IOManager *ioService, int taskIndex, vector<uint64_t *> localLeft, uint64_t limitLeft, vector<uint64_t *> localRight, uint64_t limitRight)
{
    // 解析分区列
    if (limitLeft > hashThreshold)
    {
        uint64_t *leftKeyColumn = localLeft[leftColId];
        if (cntBuilding)
        {
            // 构建计数哈希表
            hashTablesCnt[taskIndex] = new unordered_map<uint64_t, uint64_t>();
            unordered_map<uint64_t, uint64_t> *hashTable = hashTablesCnt[taskIndex];

            hashTable->reserve(limitLeft * 2);
            for (uint64_t i = 0; i < limitLeft; i++)
            {
                if (left->counted)
                    (*hashTable)[leftKeyColumn[i]] += localLeft.back()[i];
                else
                    (*hashTable)[leftKeyColumn[i]]++;
            }
        }
        else
        {
            // 构建索引哈希表
            hashTablesIndices[taskIndex] = new unordered_multimap<uint64_t, uint64_t>();
            unordered_multimap<uint64_t, uint64_t> *hashTable = hashTablesIndices[taskIndex];
            // vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];

            hashTable->reserve(limitLeft * 2);
            for (uint64_t i = 0; i < limitLeft; i++)
            {
                hashTable->emplace(make_pair(leftKeyColumn[i], i));
            }
        }
    }
    // 获取探测任务的数量和长度
    unsigned cntTask = cntProbing[taskIndex];
    uint64_t taskLength = lengthProbing[taskIndex];
    unsigned rest = restProbing[taskIndex];

    for (int i = 0; i < cntTask; i++)
    {
        tmpResults[taskIndex].emplace_back();
    }

    __sync_fetch_and_add(&pendingProbing, cntTask);
    __sync_sub_and_fetch(&pendingBuilding, 1);
    uint64_t start = 0;
    for (int i = 0; i < cntTask - 1; i++)
    {
        uint64_t length = taskLength;
        if (rest)
        {
            length++;
            rest--;
        }
        ioService->scheduler(bind(&Join::probingTask, this, ioService, taskIndex, i, localLeft, limitLeft, localRight, start, length));
        start += length;
    }
    uint64_t length = taskLength;
    if (rest)
    {
        length++;
    }
    probingTask(ioService, taskIndex, cntTask - 1, localLeft, limitLeft, localRight, start, length);
}

void Join::probingTask(monsoon::IOManager *ioService, int partIndex, int taskIndex, vector<uint64_t *> localLeft, uint64_t leftLength, vector<uint64_t *> localRight, uint64_t start, uint64_t length)
{
    // 获取右表的键列和左表的键列
    uint64_t *rightKeyColumn = localRight[rightColId];
    uint64_t *leftKeyColumn = localLeft[leftColId];
    // 存储左表和右表的数据副本
    vector<uint64_t *> copyLeftData, copyRightData;
    vector<vector<uint64_t>> &localResults = tmpResults[partIndex][taskIndex];
    uint64_t limit = start + length;
    unsigned leftColSize = requestedColumnsLeft.size();
    unsigned rightColSize = requestedColumnsRight.size();
    unsigned resultColSize = requestedColumns.size();
    unordered_map<uint64_t, uint64_t> *hashTableCnt = NULL;
    unordered_multimap<uint64_t, uint64_t> *hashTableIndices = NULL;
    unordered_map<uint64_t, uint64_t> cntMap;

    __sync_synchronize();
    if (isStopped)
    {
        goto probing_finish;
    }

    if (leftLength == 0 || length == 0)
        goto probing_finish;

    // 根据哈希表的类型，获取哈希表指针
    if (cntBuilding)
    {
        hashTableCnt = hashTablesCnt[partIndex];
    }
    else
    {
        hashTableIndices = hashTablesIndices[partIndex];
    }
    // 初始化本地结果
    for (unsigned j = 0; j < requestedColumns.size(); j++)
    {
        localResults.emplace_back();
    }
    // 如果计数标志为真，则额外初始化一个本地结果存储计数
    if (counted)
    {
        localResults.emplace_back(); // For Count Column
    }
    // 获取左表和右表请求列的数据副本
    for (auto &info : requestedColumnsLeft)
    {
        copyLeftData.push_back(localLeft[left->resolve(info)]);
    }
    if (left->counted)
    {
        copyLeftData.push_back(localLeft.back()); // Count Column for left
    }

    for (auto &info : requestedColumnsRight)
    {
        copyRightData.push_back(localRight[right->resolve(info)]);
    }
    if (right->counted)
    {
        copyRightData.push_back(localRight.back()); // Count Column for right
    }

    // 如果左表长度大于哈希阈值，则执行哈希连接
    if (leftLength > hashThreshold)
    {
        // 哈希连接
        if (cntBuilding)
        {

            for (uint64_t i = start; i < limit; i++)
            {
                // 遍历右表的键列
                auto rightKey = rightKeyColumn[i];
                // 如果右表的键值在哈希表中找不到对应的条目，则继续下一条记录
                if (hashTableCnt->find(rightKey) == hashTableCnt->end())
                    continue;
                // 获取左表匹配键值对应的计数
                uint64_t leftCnt = hashTableCnt->at(rightKey);
                // 获取右表对应行的计数，如果右表没有计数列，则默认为1
                uint64_t rightCnt = right->counted ? copyRightData[rightColSize][i] : 1;
                if (counted == 1)
                {
                    // 如果只有一列结果，则直接存储右表的键值和左表的计数乘积
                    auto data = (leftColSize == 1) ? rightKey : copyRightData[0][i];
                    localResults[0].push_back(data);
                    localResults[1].push_back(leftCnt * rightCnt);
                }
                else
                {
                    // 存储左表的键值、右表请求列的值，以及左右表计数的乘积
                    unsigned relColId = 0;
                    for (unsigned cId = 0; cId < leftColSize; ++cId) // if exist
                        localResults[relColId++].push_back(rightKey);
                    for (unsigned cId = 0; cId < rightColSize; ++cId)
                        localResults[relColId++].push_back(copyRightData[cId][i]);
                    if (counted)
                    {
                        localResults[relColId].push_back(leftCnt * rightCnt);
                    }
                }
            }
        }
        else // 索引哈希表，则按索引进行匹配
        {
            for (uint64_t i = start; i < limit; i++)
            {
                // 遍历右表的键列
                auto rightKey = rightKeyColumn[i];
                auto range = hashTableIndices->equal_range(rightKey);
                // 在哈希表中查找右表键值对应的所有左表索引
                for (auto iter = range.first; iter != range.second; ++iter)
                {
                    if (counted == 1)
                    {
                        auto &copyData((leftColSize == 1) ? copyLeftData[0] : copyRightData[0]);
                        auto rid = (leftColSize == 1) ? iter->second : i;
                        auto dup = cntMap.find(copyData[rid]);
                        uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][iter->second] : 1;
                        uint64_t rightCnt = right->counted ? copyRightData[rightColSize][i] : 1;
                        if (dup != cntMap.end())
                        {
                            localResults[1][dup->second] += leftCnt * rightCnt;
                        }
                        else
                        {
                            localResults[0].push_back(copyData[rid]);
                            localResults[1].push_back(leftCnt * rightCnt);
                            cntMap.insert(dup, pair<uint64_t, uint64_t>(copyData[rid],
                                                                        localResults[1].size() - 1));
                        }
                    }
                    else
                    {
                        unsigned relColId = 0;
                        for (unsigned cId = 0; cId < leftColSize; ++cId)
                            localResults[relColId++].push_back(copyLeftData[cId][iter->second]);
                        for (unsigned cId = 0; cId < rightColSize; ++cId)
                            localResults[relColId++].push_back(copyRightData[cId][i]);
                        if (counted)
                        {
                            uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][iter->second] : 1;
                            uint64_t rightCnt = right->counted ? copyRightData[rightColSize][i] : 1;
                            localResults[relColId].push_back(leftCnt * rightCnt);
                        }
                    }
                }
            }
        }
    }
    else
    // 如果左表长度小于等于哈希阈值，则执行嵌套循环连接 (效率低)
    {
        for (uint64_t i = 0; i < leftLength; i++)
        {
            for (uint64_t j = start; j < limit; j++)
            {
                if (leftKeyColumn[i] == rightKeyColumn[j])
                {
                    if (counted == 1)
                    {
                        auto &copyData((leftColSize == 1) ? copyLeftData[0] : copyRightData[0]);
                        auto rid = (leftColSize == 1) ? i : j;
                        auto dup = cntMap.find(copyData[rid]);
                        uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
                        uint64_t rightCnt = right->counted ? copyRightData[rightColSize][j] : 1;
                        if (dup != cntMap.end())
                        {
                            localResults[1][dup->second] += leftCnt * rightCnt;
                        }
                        else
                        {
                            localResults[0].push_back(copyData[rid]);
                            localResults[1].push_back(leftCnt * rightCnt);
                            cntMap.insert(dup, pair<uint64_t, uint64_t>(copyData[rid],
                                                                        localResults[1].size() - 1));
                        }
                    }
                    else
                    {
                        unsigned relColId = 0;
                        for (unsigned cId = 0; cId < leftColSize; ++cId)
                            localResults[relColId++].push_back(copyLeftData[cId][i]);
                        for (unsigned cId = 0; cId < rightColSize; ++cId)
                            localResults[relColId++].push_back(copyRightData[cId][j]);
                        if (counted)
                        {
                            uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
                            uint64_t rightCnt = right->counted ? copyRightData[rightColSize][j] : 1;
                            localResults[relColId].push_back(leftCnt * rightCnt);
                        }
                    }
                }
            }
        }
    }
    if (localResults[0].size() == 0)
        goto probing_finish;
    for (unsigned i = 0; i < resultColSize; i++)
    {
        results[i].addTuples(resultIndex[partIndex] + taskIndex, localResults[i].data(), localResults[i].size());
    }
    if (counted)
    {
        results[resultColSize].addTuples(resultIndex[partIndex] + taskIndex,
                                         localResults[resultColSize].data(), localResults[resultColSize].size());
    }
    __sync_fetch_and_add(&resultSize, localResults[0].size());
probing_finish:
    int remainder = __sync_sub_and_fetch(&pendingProbing, 1);
    if (UNLIKELY(remainder == 0 && pendingBuilding == 0))
    {

        for (unsigned cId = 0; cId < requestedColumns.size(); ++cId)
        {
            results[cId].fix();
        }
        if (counted)
        {
            results[requestedColumns.size()].fix();
        }
        localMemPool[allocTid]->requestFree(partitionTable[0]);
        localMemPool[allocTid]->requestFree(partitionTable[1]);
        finishAsyncRun(*ioService, true);
    }
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
// Require a column and add it to results
{
    // cerr << "SlefJoin::require" << endl;
    if (requiredIUs.count(info))
        return true;
    if (input->require(info))
    {
        requiredIUs.emplace(info);
        return true;
    }
    return false;
}
//---------------------------------------------------------------------------
void SelfJoin::asyncRun(monsoon::IOManager &ioService)
{
    //cerr << "Self::run" << endl;
    pendingAsyncOperator = 1;
    input->require(pInfo.left);
    input->require(pInfo.right);
    __sync_synchronize();
    input->asyncRun(ioService);
}
//---------------------------------------------------------------------------
void SelfJoin::selfJoinTask(monsoon::IOManager *ioService, int taskIndex, uint64_t start, uint64_t length)
{
    auto &inputData = input->getResults();
    vector<vector<uint64_t>> &localResults = tmpResults[taskIndex];
    auto leftColId = input->resolve(pInfo.left);
    auto rightColId = input->resolve(pInfo.right);

    auto leftColIt = inputData[leftColId].begin(start);
    auto rightColIt = inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;

    unsigned colSize = copyData.size();

    for (int j = 0; j < colSize; j++)
    {
        localResults.emplace_back();
    }
    if (counted && !input->counted)
    {
        localResults.emplace_back();
    }

    for (unsigned i = 0; i < colSize; i++)
    {
        colIt.push_back(copyData[i]->begin(start));
    }
    unordered_map<uint64_t, uint64_t> cntMap;
    for (uint64_t i = start, limit = start + length; i < limit; ++i)
    {
        if (*leftColIt == *rightColIt)
        {
            if (counted == 1)
            {
                auto dup = cntMap.find(*(colIt[0]));
                uint64_t dupCnt = (input->counted) ? *(colIt[1]) : 1;
                if (dup != cntMap.end())
                {
                    localResults[1][dup->second] += dupCnt;
                }
                else
                {
                    localResults[0].push_back(*(colIt[0]));
                    localResults[1].push_back(dupCnt);
                    cntMap.insert(dup, pair<uint64_t, uint64_t>(*(colIt[0]), localResults[1].size() - 1));
                }
            }
            else
            { // 2 0 / 3 0 / 3 1 / 4 0 / 4 1 ...
                // If counted is true, colSize already contains count Column
                for (unsigned cId = 0; cId < colSize; ++cId)
                {
                    localResults[cId].push_back(*(colIt[cId]));
                }
            }
        }
        ++leftColIt;
        ++rightColIt;
        for (unsigned i = 0; i < colSize; i++)
        {
            ++colIt[i];
        }
    }
    for (int i = 0; i < colSize; i++)
    {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
    if (counted && !input->counted)
    {
        results[colSize].addTuples(taskIndex, localResults[colSize].data(), localResults[colSize].size());
    }
    __sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0))
    {
        for (unsigned cId = 0; cId < colSize; ++cId)
        {
            results[cId].fix();
        }
        if (counted && !input->counted)
        {
            results[colSize].fix();
        }
        finishAsyncRun(*ioService, true);
        // input = nullptr;
    }
}
//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(monsoon::IOManager &ioService)
{
    assert(pendingAsyncOperator == 0);

    if (input->resultSize == 0)
    {
        finishAsyncRun(ioService, true);
        return;
    }

    int cntTask = THREAD_NUM;
    uint64_t taskLength = input->resultSize / cntTask;
    uint64_t rest = input->resultSize % cntTask;

    if (taskLength < minTuplesPerTask)
    {
        cntTask = input->resultSize / minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = input->resultSize / cntTask;
        rest = input->resultSize % cntTask;
    }

    auto &inputData = input->getResults();

    for (auto &iu : requiredIUs)
    {
        auto id = input->resolve(iu);
        copyData.emplace_back(&inputData[id]);
        select2ResultColId.emplace(iu, copyData.size() - 1);
        results.emplace_back(cntTask);
    }
    if (copyData.size() == 1)
    {
        auto &sInfo(*(requiredIUs.begin()));
        counted = Joiner::relations[sInfo.relId].needCount[sInfo.colId];
    }
    if (input->counted)
    {
        if (!counted)
        {
            counted = 2;
        }
        copyData.emplace_back(&inputData.back());
    }
    if (counted)
    {
        results.emplace_back(cntTask);
    }

    for (int i = 0; i < cntTask; i++)
    {
        tmpResults.emplace_back();
    }

    pendingTask = cntTask;
    __sync_synchronize();
    uint64_t start = 0;
    for (int i = 0; i < cntTask; i++)
    {
        uint64_t length = taskLength;
        if (rest)
        {
            length++;
            rest--;
        }
        ioService.scheduler(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length));
        start += length;
    }
}
//---------------------------------------------------------------------------
void Checksum::asyncRun(monsoon::IOManager &ioService, int queryIndex)
{
    //cerr << "checksum::run-->";
    this->queryIndex = queryIndex;
    pendingAsyncOperator = 1;
    for (auto &sInfo : colInfo)
    {
        input->require(sInfo);
    }
    __sync_synchronize();
    input->asyncRun(ioService);
}
//---------------------------------------------------------------------------

void Checksum::checksumTask(monsoon::IOManager *ioService, int taskIndex, uint64_t start, uint64_t length)
{
    auto &inputData = input->getResults();

    int sumIndex = 0;
    for (auto &sInfo : colInfo)
    {
        auto colId = input->resolve(sInfo);
        auto inputColIt = inputData[colId].begin(start);
        uint64_t sum = 0;
        if (input->counted)
        {
            auto countColIt = inputData.back().begin(start);
            for (int i = 0; i < length; i++, ++inputColIt, ++countColIt)
            {
                sum += (*inputColIt) * (*countColIt);
            }
        }
        else
        {
            for (int i = 0; i < length; i++, ++inputColIt)
            {
                sum += (*inputColIt);
            }
        }
        __sync_fetch_and_add(&checkSums[sumIndex++], sum);
    }

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0))
    {
        finishAsyncRun(*ioService, false);
        // input = nullptr;
    }
}

void Checksum::createAsyncTasks(monsoon::IOManager &ioService)
{
    assert(pendingAsyncOperator == 0);
    for (auto &sInfo : colInfo)
    {
        checkSums.push_back(0);
    }

    if (input->resultSize == 0)
    {
        finishAsyncRun(ioService, false);
        return;
    }

    int cntTask = THREAD_NUM;
    uint64_t taskLength = input->resultSize / cntTask;
    uint64_t rest = input->resultSize % cntTask;

    if (taskLength < minTuplesPerTask)
    {
        cntTask = input->resultSize / minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = input->resultSize / cntTask;
        rest = input->resultSize % cntTask;
    }
    pendingTask = cntTask;
    __sync_synchronize();
    uint64_t start = 0;
    for (int i = 0; i < cntTask; i++)
    {
        uint64_t length = taskLength;
        if (rest)
        {
            length++;
            rest--;
        }
        ioService.scheduler(bind(&Checksum::checksumTask, this, &ioService, i, start, length));
        start += length;
    }
}
void Checksum::finishAsyncRun(monsoon::IOManager &ioService, bool startParentAsync)
{
    joiner.asyncResults[queryIndex] = std::move(checkSums);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
    assert(pending >= 0);
    if (pending == 0)
    {
        unique_lock<mutex> lk(joiner.cvAsyncMt); // guard for missing notification
        joiner.cvAsync.notify_one();
    }
}
void Checksum::printAsyncInfo()
{
    cout << "pendingChecksum : " << pendingTask << endl;
    input->printAsyncInfo();
}

void SelfJoin::printAsyncInfo()
{
    cout << "pendingSelfJoin : " << pendingTask << endl;
    input->printAsyncInfo();
}
void Join::printAsyncInfo()
{
    cout << "pendingBuilding : " << pendingBuilding << endl;
    cout << "pendingProbing : " << pendingProbing << endl;
    cout << "pendingScattering[0] : " << pendingScattering[0] << endl;
    cout << "pendingScattering[1] : " << pendingScattering[1 * CACHE_LINE_SIZE] << endl;
    cout << "pendingMakingHistogram[0] : " << pendingMakingHistogram[0] << endl;
    cout << "pendingMakingHistogram[1] : " << pendingMakingHistogram[1 * CACHE_LINE_SIZE] << endl;
    left->printAsyncInfo();
    right->printAsyncInfo();
}
void FilterScan::printAsyncInfo()
{
    cout << "pendingFilterScan : " << pendingTask << endl;
}
void Scan::printAsyncInfo()
{
}
