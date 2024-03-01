#pragma once

#include <iostream>
#include <vector>
#include "utils.h"

template <typename T>
class Column
{

public:
    friend ColIterator;
    Column(unsigned taskNum)
    {
        baseOffset_.push_back(0);
        for (int i = 0; i < taskNum; i++)
        {
            tupels_.empalce_back();
            baseOffset_.push_back(0);
            tupleLen_.emplace_back();
        }
    }

    void addTuples(unsigned pos, uin64_t *tuple, uint64 len)
    {
        tuples_[pos] = tuple;
        tupleLen_[pos] = len;
    }
    void fix()
    {
        // issue: 前缀和？
        for (unsigned i = 1; i < baseOffset_.size(); ++i)
        {
            baseOffset_[i] = baseOffset_[i - 1] + tupleLen_[i - 1];
        }
        fixed_ = true;
    }

private:
    // 存储指向数据列的指针
    std::vector<T *> tuples_;
    // 存储数据列长度
    std::vector<uint64_t> tupleLen_;
    // 数据列初始偏移量
    std::vector<uint64_t> baseOffset_;
    bool fixed_ = false;
};

template <typename T>
class ColIterator
{
public:
    ColIterator(Column<T> &col, uint64 start)
        : col(col)
    {
        // 当前列为空
        if (!col.tuples_.size())
        {
            return;
        }
        // 从偏移量数组找第一个不小于start的值的位置
        auto it = lower_bound(col.baseOffset_.begin(), col.baseOffset_.end(), start);
        idx = it - col.baseOffset_.begin();
        assert(idx_ < col.baseOffset_.size());
        if (col.baseOffset_[idx_] != start)
        {
            idx++;
        }
        while (0 == col.tupleLen_[idx_])
        {
            // 移动到下一列
            idx_++;
            offset_ = 0;
            if (idx_ == col.tupleLen_.size())
            {
                CondPanic(false, "invalid start value for cols") break;
                break;
            }
        }
    }
    /// @brief 返回迭代器开始位置
    /// @param idx: 开始位置
    /// @return 列首迭代器
    ColIterator begin(uint64_t idx)
    {
        assert(fixed_);
        return ColIterator(*this, idx);
    }

    ColIterator end()
    {
    }

    T &operator*()
    {
        CondPanic(idx_ < col.tupleLen_.size(), "invalid idx for cols");
        return col_.tuples_[idx_][offset_];
    }

    ColIterator &operator++()
    {
        offset_++;
        while (offset_ >= col_.tupleLen_[idx_])
        {
            idx_++;
            offset_ = 0;
            if (idx_ == col_.tupleLen_.size())
            {
                break;
            }
        }
        return *this;
    }

private:
    // 当前列的索引
    unsigned idx_;
    // 访问列的当前位置
    uint64_t offset_;
    Column<T> col_;
};