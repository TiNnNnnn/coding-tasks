#pragma once

#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <functional>
#include <condition_variable>

class ThreadPool
{
public:
    ThreadPool(size_t numThreads) : running(true)
    {
        for (size_t i = 0; i < numThreads; ++i)
        {
            workers.emplace_back([this]()
                                 {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        condition.wait(lock, [this]() { return !running || !tasks.empty(); });
                        if (!running && tasks.empty()) {
                            return;
                        }
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                } });
        }
    }

    ~ThreadPool()
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            running = false;
        }
        condition.notify_all();
        for (auto &worker : workers)
        {
            worker.join();
        }
    }

    template <typename F, typename... Args>
    void addTask(F &&f, Args &&...args)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            tasks.emplace([=]()
                          { f(args...); });
        }
        condition.notify_one();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex mutex;
    std::condition_variable condition;
    bool running;
};

// int main()
// {
//     ThreadPool pool(4); // 创建一个包含4个线程的线程池

//     // 添加任务到线程池
//     for (int i = 0; i < 8; ++i)
//     {
//         pool.addTask([=]()
//                      {
//             std::this_thread::sleep_for(std::chrono::seconds(1));
//             std::cout << "Task " << i << " executed by thread " << std::this_thread::get_id() << std::endl; });
//     }

//     std::this_thread::sleep_for(std::chrono::seconds(5));

//     return 0;
// }
