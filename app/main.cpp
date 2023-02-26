#include <dbThreadPool.hpp>
#include <logger.hpp>
#include <dbBenchmark.hpp>


int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_LEVEL_INFO));
    dbThreadPoolInit();

    DBBenchmark::leveldbBenchmark();

    return 0;
}
