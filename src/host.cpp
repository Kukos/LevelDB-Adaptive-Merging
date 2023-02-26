#include <host.hpp>
#include <logger.hpp>

#include <thread>
#include <fstream>
#include <iostream>

#ifdef __linux__

#include <unistd.h>

#elif _WIN32

#endif

namespace hostPlatform
{
// LINUX platform
#ifdef __linux__

std::string directorySeparator = std::string("/");

void flushFileSystemCache()
{
    LOGGER_LOG_DEBUG("Flushing Linux file system buffer/cache (see free --giga)");

    sync(); // sync all of the files with disk

    std::ofstream drop_cache_file;
    drop_cache_file.open("/proc/sys/vm/drop_caches"); // flush fs cache
    if (!drop_cache_file.is_open())
    {
        std::cerr << "Cannot open /proc/sys/vm/drop_caches file, please run this app as root" << std::endl;
        return;
    }


    drop_cache_file << "3";
    drop_cache_file.close();

    // wait for flushing
    std::this_thread::sleep_for(std::chrono::seconds(10));
}

// WINDOWS platform
#elif _WIN32

std::string directorySeparator = std::string("\\");

void flushFileSystemCache()
{
    LOGGER_LOG_DEBUG("Flushing Windows file system is not implemented!");

    //TODO
}

#endif



}
