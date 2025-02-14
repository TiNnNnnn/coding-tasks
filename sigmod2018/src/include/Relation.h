#pragma once
#include <cstdint>
#include <string>
#include <vector>

using RelationId = unsigned;
//---------------------------------------------------------------------------
class Relation {
private:
    /// Owns memory (false if it was mmaped)
    bool ownsMemory;
    /// Loads data from a file
    void loadRelation(const char* fileName);

public:
    /// The number of tuples
    uint64_t size;
    /// The join column containing the keys
    std::vector<uint64_t*> columns;
    //存储当前表的每一列中[元素，元素数量]
    std::vector<std::vector<uint64_t*>> counted;
    //统计当前列是否需要进行count预先统计
    std::vector<int> needCount;

    /// Stores a relation into a file (binary)
    void storeRelation(const std::string& fileName);
    /// Stores a relation into a file (csv)
    void storeRelationCSV(const std::string& fileName);
    /// Dump SQL: Create and load table (PostgreSQL)
    void dumpSQL(const std::string& fileName,unsigned relationId);

    void loadStat(unsigned colId);

    /// Constructor without mmap
    Relation(uint64_t size,std::vector<uint64_t*>&& columns) : ownsMemory(true), size(size), columns(columns) {}
    /// Constructor using mmap
    Relation(const char* fileName);
    /// Delete copy constructor
    Relation(const Relation& other)=delete;
    /// Move constructor
    Relation(Relation&& other)=default;
    /// The destructor
    ~Relation();
};
//---------------------------------------------------------------------------
