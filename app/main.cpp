#include <dbThreadPool.hpp>
#include <logger.hpp>

int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_LEVEL_INFO));

    dbThreadPoolInit();

    return 0;
}
