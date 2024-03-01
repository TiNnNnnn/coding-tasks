#include <iostream>

#include "joiner.h"
#include "parser.h"
#include "config.h"

int main(int argc, char *argv[])
{
    Joiner joiner(THREAD_NUM);

    // Read join relations
    std::string line;
    while (getline(std::cin, line))
    {
        if (line == "Done")
            break;
        joiner.addRelation(line.c_str());
    }
    joiner.loadStat();

    // Preparation phase (not timed)
    // Build histograms, indexes,...

    while (getline(std::cin, line))
    {
        // 结束输入
        if (line == "F")
        {
            // 等待所有任务结束
            joiner.waitAsyncJoins();
            // 获取连接结果
            auto results = joiner.getAsyncJoinResults();
            for (auto &res : results)
            {
                std::cout << res;
            }
            continue;
        }
        // 创建异步查询任务
        joiner.createAsyncQueryTask(line);
    }

    // QueryInfo i;
    //  while (getline(std::cin, line))
    //  {
    //      if (line == "F")
    //          continue; // End of a batch
    //      //i.parseQuery(line);
    //      //std::cout << joiner.join(i);
    //  }

    return 0;
}
