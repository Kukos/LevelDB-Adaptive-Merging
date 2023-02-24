#ifndef LOGGER_HPP
#define LOGGER_HPP

// if level is not defined in Makefile then setup default value: INFO
#ifndef LOGGER_ACTIVE_LEVEL
#define LOGGER_ACTIVE_LEVEL 2
#endif

// overwrite spdlog level with our new log level
#define SPDLOG_ACTIVE_LEVEL LOGGER_ACTIVE_LEVEL

// here put all includes needed from spdlog
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

enum logger_levels
{
    LOGGER_LEVEL_TRACE = SPDLOG_LEVEL_TRACE,
    LOGGER_LEVEL_DEBUG = SPDLOG_LEVEL_DEBUG,
    LOGGER_LEVEL_INFO = SPDLOG_LEVEL_INFO,
    LOGGER_LEVEL_WARN = SPDLOG_LEVEL_WARN,
    LOGGER_LEVEL_ERROR = SPDLOG_LEVEL_ERROR,
    LOGGER_LEVEL_CRITICAL = SPDLOG_LEVEL_CRITICAL,
    LOGGER_LEVEL_OFF = SPDLOG_LEVEL_OFF
};

extern spdlog::string_view_t logger_level_names[];
#define LOGGER_LEVEL_NAMES logger_level_names

/**
 * @brief Start logging to stdout and into file.
 *        Function reads LOGGER_ACTIVE_LEVEL and sets logger level to this level
 *
 */
void loggerStart();

/**
 * @brief Set logger Level to new level
 *
 * @param [in] level - logger_level
 */
void loggerSetLevel(enum logger_levels level);

// Use this macros to log. This macro insert function:line automaticly
#define LOGGER_LOG_TRACE(...)    SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::trace, __VA_ARGS__)
#define LOGGER_LOG_DEBUG(...)    SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::debug, __VA_ARGS__)
#define LOGGER_LOG_INFO(...)     SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::info, __VA_ARGS__)
#define LOGGER_LOG_WARN(...)     SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::warn, __VA_ARGS__)
#define LOGGER_LOG_ERROR(...)    SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::err, __VA_ARGS__)
#define LOGGER_LOG_CRITICAL(...) SPDLOG_LOGGER_CALL(spdlog::default_logger_raw(), spdlog::level::critical, __VA_ARGS__)

#endif