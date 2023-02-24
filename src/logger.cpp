#include <logger.hpp>
#include <host.hpp>

#include <iomanip>
#include <ctime>
#include <sstream>

spdlog::string_view_t logger_level_names[] = SPDLOG_LEVEL_NAMES;

void loggerSetLevel(const enum logger_levels level)
{
    spdlog::set_level(static_cast<spdlog::level::level_enum>(level));
}

void loggerStart()
{
    // Create fileName with current date
    const time_t t = std::time(nullptr);
    tm tm = *std::localtime(&t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%d-%m-%Y.%H:%M:%S");

    // ./logs/log_XXX.log
    const std::string logFileName = std::string(".") + hostPlatform::directorySeparator +
                                    std::string("logs") + hostPlatform::directorySeparator +
                                    std::string("log_") + oss.str() + std::string(".log");

    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));
    LOGGER_LOG_INFO("SPDLOG in version {}.{}.{} found", SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR, SPDLOG_VER_PATCH);

    // stdout + file
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_st>());
    sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_st>(logFileName));
    std::shared_ptr<spdlog::logger> logger = std::make_shared<spdlog::logger>("", begin(sinks), end(sinks));

    // overwrite namespaced logger by my logger (without this we need to get this logger each time, now we can use spdlog::info)
    spdlog::set_default_logger(logger);

    LOGGER_LOG_INFO("Setting LEVEL to {}", LOGGER_LEVEL_NAMES[LOGGER_ACTIVE_LEVEL]);
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));

    LOGGER_LOG_INFO("Logging to file {}", logFileName);
    LOGGER_LOG_INFO("Logger is ready!");
}