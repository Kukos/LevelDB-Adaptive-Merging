#ifndef HOST_HPP
#define HOST_HPP

#include <string>

namespace hostPlatform
{
extern std::string directorySeparator;
void flushFileSystemCache();

// LINUX platform
#ifdef __linux__

// WINDOWS platform
#elif _WIN32

#endif
}

#endif