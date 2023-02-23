#ifndef DB_THREAD_POOL_HPP
#define DB_THREAD_POOL_HPP

#include <memory>
#include <thread>

#include <threadPool.hpp>

class DBThreadPool
{
public:
    thread_pool threadPool;
    std::mutex mutex;

    DBThreadPool(size_t maxThreads) noexcept(true)
    : threadPool(maxThreads)
    {

    }

};

extern std::unique_ptr<DBThreadPool> dbThreadPool;

static inline void dbThreadPoolInit(size_t maxThreads)
{
    dbThreadPool = std::make_unique<DBThreadPool>(maxThreads);
}

static inline void dbThreadPoolInit()
{
    dbThreadPool = std::make_unique<DBThreadPool>(std::thread::hardware_concurrency());
}

// Example of usage:

// #include <dbThreadPool.hpp>

// dbThreadPoolInit(8); // to use max 8 threads
// dbThreadPoolInit(); // to use max machine threads (cores / hyper threading)
// std::vector<std::future<bool>> futures;
// auto f = [](const std::string& toPrint)
// {
//     std::cout << toPrint << std::endl;
// };

// futures.push_back(dbThreadPool->threadPool.submit(f, "ala "));
// futures.push_back(dbThreadPool->threadPool.submit(f, "ma "));
// futures.push_back(dbThreadPool->threadPool.submit(f, "kota "));
// futures.push_back(dbThreadPool->threadPool.submit(f, "."));

// for (auto& task : futures)
//     task.get();


#endif
