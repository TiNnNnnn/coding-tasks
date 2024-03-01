#include "relation.h"
#include "config.h"
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unordered_set>
#include <unordered_map>

// Stores a relation into a binary file
void Relation::storeRelation(const std::string &file_name)
{
    std::ofstream out_file;
    out_file.open(file_name, std::ios::out | std::ios::binary);
    // 写入元组数量
    out_file.write((char *)&size_, sizeof(size_));
    auto numColumns = columns_.size();
    // 写入列数
    out_file.write((char *)&numColumns, sizeof(size_t));
    for (auto c : columns_)
    {
        // 按列写入文件
        out_file.write((char *)c, size_ * sizeof(uint64_t));
    }
    out_file.close();
}

// Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
void Relation::storeRelationCSV(const std::string &file_name)
{
    std::ofstream out_file;
    out_file.open(file_name + ".tbl", std::ios::out);
    for (uint64_t i = 0; i < size_; ++i)
    {
        for (auto &c : columns_)
        {
            out_file << c[i] << '|';
        }
        out_file << "\n";
    }
}

// Dump SQL: Create and load table (PostgreSQL)
void Relation::dumpSQL(const std::string &file_name, unsigned relation_id)
{
    std::ofstream out_file;
    out_file.open(file_name + ".sql", std::ios::out);
    // Create table statement
    out_file << "CREATE TABLE r" << relation_id << " (";
    for (unsigned cId = 0; cId < columns_.size(); ++cId)
    {
        out_file << "c" << cId << " bigint"
                 << (cId < columns_.size() - 1 ? "," : "");
    }
    out_file << ");\n";
    // Load from csv statement
    out_file << "copy r" << relation_id << " from 'r" << relation_id
             << ".tbl' delimiter '|';\n";
}

// 加载relation到内存
void Relation::loadRelation(const char *file_name)
{
    int fd = open(file_name, O_RDONLY);
    if (fd == -1)
    {
        std::cerr << "cannot open " << file_name << std::endl;
        throw;
    }

    // Obtain file size_
    struct stat sb
    {
    };
    if (fstat(fd, &sb) == -1)
        std::cerr << "fstat\n";

    auto length = sb.st_size;

    char *addr = static_cast<char *>(mmap(nullptr,
                                          length,
                                          PROT_READ,
                                          MAP_PRIVATE,
                                          fd,
                                          0u));
    if (addr == MAP_FAILED)
    {
        std::cerr << "cannot mmap " << file_name << " of length " << length
                  << std::endl;
        throw;
    }

    if (length < 16)
    {
        std::cerr << "relation_ file " << file_name
                  << " does not contain a valid header"
                  << std::endl;
        throw;
    }
    /*
        uint64_t numTuples|uint64_t numColumns|
        uint64_t T0C0|uint64_t T1C0|..|uint64_t TnC0|uint64_t T0C1|
        ..|uint64_t TnC1|..|uint64_t TnCm
    */
    // 读取tuple数量
    this->size_ = *reinterpret_cast<uint64_t *>(addr);
    addr += sizeof(size_);
    // 读取列数
    auto numColumns = *reinterpret_cast<size_t *>(addr);
    addr += sizeof(size_t);
    // 读取数据部分,存入columes_
    for (unsigned i = 0; i < numColumns; ++i)
    {
        this->columns_.push_back(reinterpret_cast<uint64_t *>(addr));
        addr += size_ * sizeof(uint64_t);
    }
}

// Constructor that loads relation_ from disk
Relation::Relation(const char *file_name)
    : owns_memory_(false), size_(0)
{
    loadRelation(file_name);
}

// Destructor
Relation::~Relation()
{
    if (owns_memory_)
    {
        for (auto c : columns_)
            delete[] c;
    }
}

void Relation::loadStat(unsigned colId)
{
    // 获取指定的列
    auto &c(columns_[colId]);
    std::unordered_set<uint64_t> cntSet;
    // 计算统计采样值，确保采样的元组数量不会超过指定的数量
    const unsigned stat_sample = size_ / SAMPLING_CNT == 0 ? 1 : size_ / SAMPLING_CNT;
    //
    for (unsigned i = 0; i < size_; i += stat_sample)
    {
        cntSet.insert(c[i]);
    }
    bool res = ((size_ / stat_sample / cntSet.size()) >= COUNT_THRESHOLD);
    if (res)
    {
        std::unordered_map<uint64_t, uint64_t> cntMap;
        for (unsigned i = 0; i < size_; i++)
        {
            ++cntMap[c[i]];
        }
        uint64_t sizep = 0;
        uint64_t *vals = new uint64_t[cntMap.size() * 2];
        uint64_t *cnts = vals + cntMap.size();
        for (auto &it : cntMap)
        {
            vals[sizep] = it.first;
            cnts[sizep] = it.second;
            sizep++;
        }
        counted_[colId].emplace_back(vals);
        counted_[colId].emplace_back(cnts);
    }
    needCount_[colId] = res;
}
