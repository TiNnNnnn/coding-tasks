#include <iostream>
#include <mutex>
#include <vector>

template <typename T>
class concurrent_vector
{
private:
    std::vector<T> data;
    std::mutex mtx;

public:
    // 添加元素到 vector
    void push_back(const T &value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        data.push_back(value);
    }

    // 获取 vector 的大小
    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return data.size();
    }

    // 访问 vector 中的元素
    const T &operator[](size_t index) const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return data[index];
    }

    // 删除 vector 中的元素
    void erase(size_t index)
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (index < data.size())
        {
            data.erase(data.begin() + index);
        }
    }

    // 返回vector的起始迭代器
    typename std::vector<T>::iterator begin()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return data.begin();
    }

    // 返回vector的结束迭代器
    typename std::vector<T>::iterator end()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return data.end();
    }

    // 清空 vector
    void clear()
    {
        std::lock_guard<std::mutex> lock(mtx);
        data.clear();
    }
};