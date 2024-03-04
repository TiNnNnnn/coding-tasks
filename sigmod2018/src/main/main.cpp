#include <iostream>
#include <thread>
#include "Joiner.h"
#include "Parser.h"
#include "Config.h"

using namespace std;

int main(int argc, char *argv[])
{
    Joiner joiner(THREAD_NUM);
    // 将表数据载入内存
    string line;
    while (getline(cin, line))
    {
        if (line == "Done")
            break;
        // cerr << "line: " << line << endl;
        joiner.addRelation(line.c_str());
    }
    joiner.loadStat();
    // 读入查询语句
    //int d = 0;
    while (getline(cin, line))
    {
        // if (d == 0)
        // {
        //     d++;
        //     continue;
        // }
        if (line == "F")
        { // End of a batch
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto &result : results)
                cout << result;
            continue;
        }
        joiner.createAsyncQueryTask(line);
        //d++;
    }
    return 0;
}
