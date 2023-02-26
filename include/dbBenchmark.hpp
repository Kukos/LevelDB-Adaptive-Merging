#ifndef DB_BENCHMARK_HPP
#define DB_BENCHMARK_HPP

#include <dbRecord.hpp>
#include <dbWriteBuffer.hpp>

#include <vector>

class DBBenchmark
{
private:
    static std::vector<size_t> generateAMQueries(size_t databaseEntries, double sel) noexcept(true);

public:
    static void leveldbBenchmarkPut(const std::vector<DBRecord>& entries, size_t millisecondsSleep, bool flushFileSystemBuffer = true) noexcept(true);
    static void leveldbBenchmarkWritebatch(const std::vector<DBRecord>& entries, size_t batchSize, size_t millisecondsSleep, bool flushFileSystemBuffer = true) noexcept(true);
    static void leveldbBenchmarkAMSimulation(const std::vector<DBRecord>& entries, double sel, size_t millisecondsSleep, bool flushFileSystemBuffer = true) noexcept(true);
    static void leveldbBenchmarkAMSimulationWithWriteBuffer(const std::vector<DBRecord>& entries, size_t bufferSize, double sel, size_t millisecondsSleep, bool flushFileSystemBuffer = true) noexcept(true);

    static void leveldbBenchmark() noexcept(true);
};

#endif