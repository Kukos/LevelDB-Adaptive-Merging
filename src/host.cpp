#include <host.hpp>

namespace hostPlatform
{
// LINUX platform
#ifdef __linux__

std::string directorySeparator = std::string("/");

// WINDOWS platform
#elif _WIN32

std::string directorySeparator = std::string("\\");

#endif
}
