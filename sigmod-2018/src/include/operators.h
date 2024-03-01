#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <set>

#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <mutex>

#include "col.h"
#include "relation.h"
#include "parser.h"
#include "config.h"

namespace std
{
    /// Simple hash function to enable use with unordered_map
    template <>
    struct hash<SelectInfo>
    {
        std::size_t operator()(SelectInfo const &s) const noexcept
        {
            return s.binding ^ (s.col_id << 5);
        }
    };
};

class Joiner;

/// Operators materialize their entire result
class Operator
{
    friend Joiner;

protected:
    // 父操作
    std::weak_ptr<Operator> parent_;
    /// select info 到 result的映射
    std::unordered_map<SelectInfo, unsigned> select_to_result_col_id_;
    /// The materialized results
    std::vector<Column<uint64_t>> result_columns_;
    // std::vector<uint64_t *> result_columns_;
    /// 中间结果
    // std::vector<std::vector<uint64_t>> tmp_results_;
    /// The result size
    // uint64_t result_size_ = 0;

    // 当为0时，表示所有异步运行的输入操作已经完成
    int pendingAsyncOperator_ = -1;

    virtual void finishAsyncRun(
        boost::asio::io_service &ios,
        bool startParentAsync = false);

    uint64_t result_size_ = 0;

public:
    bool isStopped_ = false;
    // 请求列并，并加入结果集中
    virtual bool require(SelectInfo info) = 0;
    // 解析列
    unsigned resolve(SelectInfo info)
    {
        assert(select_to_result_col_id_.find(info) != select_to_result_col_id_.end());
        return select_to_result_col_id_[info];
    }
    /// Run
    // virtual void run() = 0;
    /// Get  materialized results
    // virtual std::vector<uint64_t *> getResults();

    // 异步运行
    virtual void asynRun(boost::asio::io_service &ios) = 0;
    virtual void createAsyncTasks(boost::asio::io_service &ios) { throw; }
    // 设置父操作
    void setParent(std::shared_ptr<Operator> p) { this->parent_ = p; }
    // 获取运行结果大小
    uint64_t result_size() const { return result_size_; }
    // 获取材料化结果
    virtual std::vector<Column<uint64_t>> &getResults();
    // 获取材料化结果大小
    virtual uint64_t getResultsSize();
    // 停止所有子操作
    virtual void stop()
    {
        isStopped_ = true;
        __sync_synchronize();
    }

    // 打印异步信息
    virtual void printAsyncInfo() = 0;
    virtual ~Operator() {}
};

class Scan : public Operator
{
protected:
    /// 关系
    const Relation &relation_;
    /// 查询中的关系名
    unsigned relation_binding_;
    // require info
    std::vector<SelectInfo> infos;

public:
    Scan(const Relation &r, unsigned relation_binding)
        : relation_(r), relation_binding_(relation_binding){};
    /// 请求列并，并加入结果集中
    bool require(SelectInfo info) override;
    virtual void asynRun(boost::asio::io_service &ios) override;
    /// Get  materialized results
    // virtual std::vector<uint64_t *> getResults() override;
    //  获取材料化结果大小
    virtual uint64_t getResultsSize() override;
    virtual void printAsyncInfo() override;
};

class FilterScan : public Scan
{
private:
    /// 过滤器信息
    std::vector<FilterInfo> filters_;
    /// 输入数据
    std::vector<uint64_t *> input_data_;
    // 临时中间结果
    std::vector<std::vector<std::vector<uint64_t>>> tmp_results;
    // 并行任务数量
    int pendingTask_ = -1;

    unsigned minTuplesPerTask = 1000;

private:
    /// 应用过滤器
    bool applyFilter(uint64_t id, FilterInfo &f);
    /// 将tuple复制到结果中
    void copy2Result(uint64_t id);

    void filterTask(boost::asio::io_service, int taskIdx, uint64_t start, uint64_t len);

public:
    FilterScan(const Relation &r, std::vector<FilterInfo> filters)
        : Scan(r,
               filters[0].filter_column.binding),
          filters_(filters){};
    FilterScan(const Relation &r, FilterInfo &filter_info)
        : FilterScan(r,
                     std::vector<
                         FilterInfo>{
                         filter_info}){};

    bool require(SelectInfo info) override;
    /// Run
    // void run() override;
    virtual void asynRun(boost::asio::io_service &ios) override;
    virtual uint64_t getResultsSize() override
    {
        return Operator::getResultsSize();
    }
    virtual void createAsyncTasks(boost::asio::io_service &ios) { throw; }
    virtual void printAsyncInfo() override;
    // virtual std::vector<uint64_t *> getResults() override
    // {
    //     return Operator::getResults();
    // }
};

class Join : public Operator
{
private:
    // 输入操作符
    std::unique_ptr<Operator> left_, right_;
    /// join 条件信息
    PredicateInfo p_info_;
    // 临时中间结果
    std::vector<std::vector<std::vector<uint64_t>>> tmp_results;
    // 关键列
    unsigned leftColId_, rightColId_;

    char pad1[CACHE_LINE_SIZE];                      // 缓存行填充，用于避免伪共享问题
    int pendingMakingHistogram[2 * CACHE_LINE_SIZE]; // 用于标记直方图生成是否完成的计数器，避免伪共享问题
    int pendingScattering[2 * CACHE_LINE_SIZE];      // 用于标记散列操作是否完成的计数器，避免伪共享问题，CACHE_LINE_SIZE/4足够
    int pendingPartitioning = -1;                    // 标记分区操作是否完成的计数器

    char pad2[CACHE_LINE_SIZE]; // 缓存行填充
    int pendingBuilding = -1;   // 标记建立操作是否完成的计数器
    char pad3[CACHE_LINE_SIZE]; // 缓存行填充
    int pendingProbing = 0;     // 标记探测操作是否完成的计数器
    char pad4[CACHE_LINE_SIZE]; // 缓存行填充

    // 顺序分配的地址用于分区，将在结果材料化后释放
    uint64_t *partitionTable[2] = {NULL, NULL}; // 存储分区表
    int allocTid = -1;                          // 分区的线程ID
    const uint64_t partitionSize = L2_SIZE / 8; // 分区大小
    uint64_t cntPartition;                      // 分区数量

    const unsigned hashThreshold = 4; // 当左边大小超过此阈值时，不使用哈希，否则使用简单比较

    // 每个分区的变量
    std::vector<unsigned> cntProbing;         // 在直方图生成中确定的探测计数器
    std::vector<uint64_t> lengthProbing;      // 探测计数器的长度
    std::vector<unsigned> restProbing;        // 探测计数器的剩余数量
    std::vector<unsigned> resultIndex;        // 结果索引
    uint64_t taskLength[2];                   // 任务长度
    uint64_t taskRest[2];                     // 任务剩余
    const unsigned minTuplesPerTask = 100000; // 最小分区表大小
                                              //    const unsigned minTuplesPerProbing = 1000; // 最小探测表大小

    std::vector<std::vector<uint64_t *>> partition[2]; // 指向partitionTable[]的分区表，直方图生成后构建，每个分区指向列的位置
    std::vector<std::vector<uint64_t>> histograms[2];  // [LR][taskIndex][partitionIndex]，每个分区的直方图向堆中分配，否则可能会导致存储无效
    std::vector<uint64_t> partitionLength[2];          // 每个分区的元组数量

    std::vector<std::unordered_multimap<uint64_t, uint64_t> *> hashTablesIndices; // 用于使用线程本地存储的哈希表索引
    std::vector<std::unordered_map<uint64_t, uint64_t> *> hashTablesCnt;          // 用于使用线程本地存储的哈希表计数
    bool cntBuilding = false;                                                     // 计数生成是否完成

    // 直方图任务
    void histogramTask(boost::asio::io_service *ioService, int cntTask, int taskIndex, int leftOrRight, uint64_t start, uint64_t length);
    // 散列任务
    void scatteringTask(boost::asio::io_service *ioService, int taskIndex, int leftOrRight, uint64_t start, uint64_t length);
    // 建立任务
    void buildingTask(boost::asio::io_service *ioService, int taskIndex, std::vector<uint64_t *> left, uint64_t leftLimit, std::vector<uint64_t *> right, uint64_t rightLimit);
    // 探测任务
    void probingTask(boost::asio::io_service *ioService, int partIndex, int taskIndex, std::vector<uint64_t *> left, uint64_t leftLength, std::vector<uint64_t *> right, uint64_t start, uint64_t length);

    /// 需要材料化的列
    std::unordered_set<SelectInfo> requestedColumns; // 请求的列
    /// 已请求的左/右列
    std::vector<SelectInfo> requestedColumnsLeft, requestedColumnsRight; // 请求的左/右列

    /// 左右输入数据的全部数据
    std::vector<Column<uint64_t>> leftInputData, rightInputData; // 左右输入数据的全部数据

    // using HT = std::unordered_multimap<uint64_t, uint64_t>;
    //  /// The hash table for the join
    //  HT hash_table_;
    //  /// Columns that have to be materialized
    //  std::unordered_set<SelectInfo> requested_columns_;
    //  /// Left/right columns that have been requested
    //  std::vector<SelectInfo> requested_columns_left_, requested_columns_right_;

    // /// The entire input data of left and right
    // std::vector<uint64_t *> left_input_data_, right_input_data_;
    // /// The input data that has to be copied
    // std::vector<uint64_t *> copy_left_data_, copy_right_data_;

private:
    /// 将tuple复制到结果
    void copy2Result(uint64_t left_id, uint64_t right_id);
    /// 为绑定创建映射关系
    void createMappingForBindings();

public:
    Join(std::unique_ptr<Operator> &&left,
         std::unique_ptr<Operator> &&right,
         const PredicateInfo &p_info)
        : left_(std::move(left)), right_(std::move(right)), p_info_(p_info){};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    // void run() override;
    virtual void asynRun(boost::asio::io_service &ios) override;
    virtual void createAsyncTasks(boost::asio::io_service &ios) { throw; }
    virtual void printAsyncInfo() override;
    // 停止所有子操作
    virtual void stop()
    {
        isStopped_ = true;
        __sync_synchronize();
        if (!left_->isStopped_)
            left_->stop();
        if (!right_->isStopped_)
            right_->stop();
    }
};

class SelfJoin : public Operator
{
private:
    /// The input operators
    std::unique_ptr<Operator> input_;
    /// The join predicate info
    PredicateInfo p_info_;
    /// The required IUs
    std::set<SelectInfo> required_IUs_;

    /// The entire input data
    std::vector<uint64_t *> input_data_;
    /// The input data that has to be copied
    std::vector<uint64_t *> copy_data_;

private:
    /// Copy tuple to result
    void copy2Result(uint64_t id);

public:
    /// The constructor
    SelfJoin(std::unique_ptr<Operator> &&input, PredicateInfo &p_info)
        : input_(std::move(input)), p_info_(p_info){};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    // void run() override;
};

class Checksum : public Operator
{
private:
    /// The input operator
    std::unique_ptr<Operator> input_;
    /// The join predicate info
    const std::vector<SelectInfo> col_info_;

    std::vector<uint64_t> check_sums_;

public:
    /// The constructor
    Checksum(std::unique_ptr<Operator> &&input,
             std::vector<SelectInfo> col_info)
        : input_(std::move(input)), col_info_(std::move(col_info)){};
    /// Request a column and add it to results
    bool require(SelectInfo info) override
    {
        // check sum is always on the highest level
        // and thus should never request anything
        throw;
    }
    /// Run
    // void run() override;

    const std::vector<uint64_t> &check_sums()
    {
        return check_sums_;
    }
};
