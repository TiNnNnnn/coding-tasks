#pragma once

#include <iostream>
#include <vector>
#include "utils.h"

template <typename T>
class Column
{
public:
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

    

private:
    // 存储指向数据列的指针
    std::vector<T *> tuples_;
    // 存储数据列长度
    std::vector<uint64_t> tupleLen_;
    // 数据列初始偏移量
    std::vector<uint64_t> baseOffset_;
    bool fixed = false;
};