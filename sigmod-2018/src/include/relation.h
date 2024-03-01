#pragma once

#include <cstdint>
#include <string>
#include <vector>

using RelationId = unsigned;

class Relation
{
private:
    /// 拥有内存标志 (false if it was mmaped)
    bool owns_memory_;

public:
    /// 元组数量
    uint64_t size_;
    /// 列指针数组 (元素地址)
    std::vector<uint64_t *> columns_;
    // 计算结果数组
    std::vector<std::vector<uint64_t *>> counted_;
    // 需要计数的列索引数组
    std::vector<int> needCount_;

    /// Constructor without mmap
    Relation(uint64_t size, std::vector<uint64_t *> &&columns)
        : owns_memory_(true), size_(size), columns_(columns) {}
    /// Constructor using mmap
    explicit Relation(const char *file_name);
    /// Delete copy constructor
    Relation(const Relation &other) = delete;
    /// Move constructor
    Relation(Relation &&other) = default;

    /// The destructor
    ~Relation();

    /// Stores a relation into a file (binary)
    void storeRelation(const std::string &file_name);
    /// Stores a relation into a file (csv)
    void storeRelationCSV(const std::string &file_name);
    /// Dump SQL: Create and load table (PostgreSQL)
    void dumpSQL(const std::string &file_name, unsigned relation_id);

    void loadStat(unsigned colId);

    /// The number of tuples
    uint64_t size() const
    {
        return size_;
    }
    /// The join column containing the keys
    const std::vector<uint64_t *> &columns() const
    {
        return columns_;
    }

private:
    /// Loads data from a file
    void loadRelation(const char *file_name);
};
