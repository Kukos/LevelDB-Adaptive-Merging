#include <dbBenchmark.hpp>
#include <logger.hpp>
#include <dbRecordGenerator.hpp>
#include <host.hpp>

#include <numeric>
#include <random>
#include <algorithm>
#include <filesystem>
#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

std::vector<size_t> DBBenchmark::generateAMQueries(const size_t databaseEntries, const double sel) noexcept(true)
{
    constexpr double rewriteTreshold = 0.05;
    const size_t rewriteTresholdEntries = static_cast<size_t>(static_cast<double>(databaseEntries) * rewriteTreshold);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> distr(0, databaseEntries);

    std::vector<size_t> queries;
    size_t entries_to_write = databaseEntries;
    while (entries_to_write > 0)
    {
        const size_t entries_in_query = static_cast<size_t>(static_cast<double>(databaseEntries) * sel);

        size_t entries_to_index = 0;
        for (size_t i = 0; i < entries_in_query; ++i)
        {
            const size_t rn = distr(gen);
            if (rn < entries_to_write)
                ++entries_to_index;
        }

        entries_to_index = std::min(entries_to_index, entries_to_write);
        queries.push_back(entries_to_index);
        entries_to_write -= entries_to_index;

        if (entries_to_write < rewriteTresholdEntries)
        {
            queries.push_back(entries_to_write);
            entries_to_write = 0;
        }
    }

    LOGGER_LOG_TRACE("generated {} AM queries for sel {} and entries {}", queries.size(), sel, databaseEntries);

    return queries;
}

void DBBenchmark::leveldbBenchmarkPut(const std::vector<DBRecord>& entries, const size_t millisecondsSleep, const bool flushFileSystemBuffer) noexcept(true)
{
    const std::string databaseFolderName = std::string(".") + hostPlatform::directorySeparator + std::string("leveldb_benchmark_put");
    std::cout << std::unitbuf;

    std::cout << "LEVELDB BENCHMARK PUT (sleep " << millisecondsSleep << " ms) ..." << std::endl;

    std::filesystem::remove_all(databaseFolderName);

    if (flushFileSystemBuffer)
        hostPlatform::flushFileSystemCache();

    // open DB
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    const leveldb::Status status = leveldb::DB::Open(options, databaseFolderName, &db);

    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database: " << status.ToString() << std::endl;
        return;
    }

    const auto startWrite = std::chrono::high_resolution_clock::now();

    // insert entries
    for (size_t i = 0; i < entries.size(); ++i)
    {
        const leveldb::WriteOptions writeOptions;
        db->Put(writeOptions, entries[i].getKey(), entries[i].getVal());

        if (millisecondsSleep > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(millisecondsSleep));

    }

    // close DB
    delete db;

    const auto endWrite = std::chrono::high_resolution_clock::now();
    std::cout << "LEVELDB BENCHMARK PUT (sleep " << millisecondsSleep << " ms ) took " << std::chrono::duration_cast<std::chrono::milliseconds>(endWrite - startWrite).count() << " ms" << std::endl;
}

void DBBenchmark::leveldbBenchmarkWritebatch(const std::vector<DBRecord>& entries, const size_t batchSize, const size_t millisecondsSleep, const bool flushFileSystemBuffer) noexcept(true)
{
    const std::string databaseFolderName = std::string(".") + hostPlatform::directorySeparator + std::string("leveldb_benchmark_writebatch_") + std::to_string(batchSize) + std::string("_") + std::to_string(millisecondsSleep);

    std::cout << std::unitbuf;
    std::cout << "LEVELDB BENCHMARK WRITEBATCH (batch size : " << batchSize <<  " sleep " << millisecondsSleep << " ms) ..." << std::endl;

    std::filesystem::remove_all(databaseFolderName);

    if (flushFileSystemBuffer)
        hostPlatform::flushFileSystemCache();

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    // opne DB
    const leveldb::Status status = leveldb::DB::Open(options, databaseFolderName, &db);

    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database: " << status.ToString() << std::endl;
        return;
    }

    auto startWrite = std::chrono::high_resolution_clock::now();

    // insert keys
    const std::string logFileName = databaseFolderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    const size_t batchNum = (entries.size() + batchSize - 1) / batchSize;
    LOGGER_LOG_DEBUG("Entries = {}, batchSize = {}, batches = {}", entries.size(), batchSize, batchNum);

    for (size_t batch = 0; batch < batchNum; ++batch)
    {
        leveldb::WriteBatch* wb = new leveldb::WriteBatch();

        const auto startBatch = std::chrono::high_resolution_clock::now();

        size_t entriesIndex = 0;
        for (size_t i = 0; i < batchSize; ++i)
        {
            wb->Put(entries[entriesIndex].getKey(), entries[entriesIndex].getVal());
            ++entriesIndex;
        }

        const leveldb::WriteOptions write_options;
        db->Write(write_options, wb);

        if (millisecondsSleep > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(millisecondsSleep));

        delete wb;

        const auto endBatch = std::chrono::high_resolution_clock::now();
        log << std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << std::endl;
    }

    log.close();
    delete db;

    const auto endWrite = std::chrono::high_resolution_clock::now();
    std::cout << "LEVELDB BENCHMARK WRITEBATCH (batch size : " << batchSize <<  " sleep " << millisecondsSleep << " ms) took " << std::chrono::duration_cast<std::chrono::milliseconds>(endWrite - startWrite).count() << " ms" << std::endl;
}

void DBBenchmark::leveldbBenchmarkAMSimulation(const std::vector<DBRecord>& entries, const double sel, const size_t millisecondsSleep, const bool flushFileSystemBuffer) noexcept(true)
{
    const std::string databaseFolderName = std::string(".") + hostPlatform::directorySeparator + std::string("leveldb_benchmark_am_simulation_") + std::to_string(static_cast<size_t>(sel * 100)) + std::string("_") + std::to_string(millisecondsSleep);

    std::cout << std::unitbuf;
    std::cout << "LEVELDB BENCHMARK AM SIMULATION (sel : " << static_cast<size_t>(sel * 100) <<  " sleep " << millisecondsSleep << " ms) ..." << std::endl;

    std::filesystem::remove_all(databaseFolderName);

    if (flushFileSystemBuffer)
        hostPlatform::flushFileSystemCache();

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    const leveldb::Status status = leveldb::DB::Open(options, databaseFolderName, &db);

    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database: " << status.ToString() << std::endl;
        return;
    }

    const auto startWrite = std::chrono::high_resolution_clock::now();

    // insert keys
    const std::string logFileName = databaseFolderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    size_t entriesIndex = 0;
    const std::vector<size_t> amQueries = generateAMQueries(entries.size(), sel);

    LOGGER_LOG_DEBUG("Entries = {}, amQueries = {}, sel = {}", entries.size(), amQueries.size(), static_cast<size_t>(sel * 100));
    for (size_t q = 0; q < amQueries.size(); ++q)
    {
        leveldb::WriteBatch* wb = new leveldb::WriteBatch();
        const auto startBatch = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < amQueries[q]; ++i)
        {
            wb->Put(entries[entriesIndex].getKey(), entries[entriesIndex].getVal());
            ++entriesIndex;
        }

        const leveldb::WriteOptions writeOptions;
        db->Write(writeOptions, wb);

        if (millisecondsSleep > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(millisecondsSleep));

        delete wb;

        const auto endBatch = std::chrono::high_resolution_clock::now();
        log << std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << std::endl;
    }

    log.close();

    delete db;

    const auto endWrite = std::chrono::high_resolution_clock::now();
    std::cout << "LEVELDB BENCHMARK AM SIMULATION (sel : " << static_cast<size_t>(sel * 100) <<  " sleep " << millisecondsSleep << " ms) took " << std::chrono::duration_cast<std::chrono::milliseconds>(endWrite - startWrite).count() << " ms" << std::endl;
}

void DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(const std::vector<DBRecord>& entries, const size_t bufferSize, const double sel, const size_t millisecondsSleep, const bool flushFileSystemBuffer) noexcept(true)
{
    const std::string databaseFolderName = std::string(".") + hostPlatform::directorySeparator + std::string("leveldb_benchmark_am_simulation_writebuffer_") + std::to_string(bufferSize) + std::string("_") + std::to_string(static_cast<size_t>(sel * 100)) + std::string("_") + std::to_string(millisecondsSleep);

    std::cout << std::unitbuf;
    std::cout << "LEVELDB BENCHMARK AM SIMULATION WITH BUFFER (bufferSize: " << bufferSize << " sel : " << static_cast<size_t>(sel * 100) <<  " sleep " << millisecondsSleep << " ms) ..." << std::endl;

    std::filesystem::remove_all(databaseFolderName);

    if (flushFileSystemBuffer)
        hostPlatform::flushFileSystemCache();

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    const leveldb::Status status = leveldb::DB::Open(options, databaseFolderName, &db);

    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database: " << status.ToString() << std::endl;
        return;
    }

    const auto startWrite = std::chrono::high_resolution_clock::now();

    // insert keys
    const std::string logFileName = databaseFolderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    size_t entriesIndex = 0;
    const std::vector<size_t> amQueries = generateAMQueries(entries.size(), sel);

    DBWriteBuffer buffer(db, bufferSize);

    LOGGER_LOG_DEBUG("Entries = {}, amQueries = {}, bufferSize = {}, sel = {}", entries.size(), amQueries.size(), bufferSize, static_cast<size_t>(sel * 100));
    for (size_t q = 0; q < amQueries.size(); ++q)
    {
        const auto startBatch = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < amQueries[q]; ++i)
        {
            buffer.insert(entries[entriesIndex]);
            ++entriesIndex;
        }

        if (millisecondsSleep > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(millisecondsSleep));

        const auto endBatch = std::chrono::high_resolution_clock::now();
        log << std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << std::endl;
    }

    buffer.flush();
    log.close();

    delete db;

    const auto endWrite = std::chrono::high_resolution_clock::now();
    std::cout << "LEVELDB BENCHMARK AM SIMULATION WITH BUFFER (bufferSize: " << bufferSize << " sel : " << static_cast<size_t>(sel * 100) <<  " sleep " << millisecondsSleep << " ms) took " << std::chrono::duration_cast<std::chrono::milliseconds>(endWrite - startWrite).count() << " ms" << std::endl;
}


void DBBenchmark::leveldbBenchmark() noexcept(true)
{
    constexpr size_t numEntries = 10 * 1000 * 1000; // 10m
    constexpr size_t valSize = 119; // TPC-C Warehouse
    const std::vector<DBRecord> entries = DBRecordGenerator::generateRecords(numEntries, valSize);

    DBBenchmark::leveldbBenchmarkPut(entries, 0);
    // DBBenchmark::leveldbBenchmarkPut(entries, 1); // over 3h test

    DBBenchmark::leveldbBenchmarkWritebatch(entries, 100 * 1000, 0);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 200 * 1000, 0);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 500 * 1000, 0);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 1000 * 1000, 0);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 10 * 1000 * 1000, 0);

    DBBenchmark::leveldbBenchmarkWritebatch(entries, 100 * 1000, 100);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 200 * 1000, 100);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 500 * 1000, 100);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 1000 * 1000, 100);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 10 * 1000 * 1000, 100);

    DBBenchmark::leveldbBenchmarkWritebatch(entries, 100 * 1000, 500);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 200 * 1000, 500);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 500 * 1000, 500);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 1000 * 1000, 500);
    DBBenchmark::leveldbBenchmarkWritebatch(entries, 10 * 1000 * 1000, 500);

    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.01, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.02, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.03, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.04, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.05, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.1, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.15, 0);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.2, 0);

    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.01, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.02, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.03, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.04, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.05, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.1, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.15, 100);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.2, 100);

    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.01, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.02, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.03, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.04, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.05, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.1, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.15, 500);
    DBBenchmark::leveldbBenchmarkAMSimulation(entries, 0.2, 500);


    constexpr size_t bufferSize = 100 * 1000;
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.01, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.02, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.03, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.04, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.05, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.1, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.15, 0);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.2, 0);

    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.01, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.02, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.03, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.04, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.05, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.1, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.15, 100);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.2, 100);

    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.01, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.02, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.03, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.04, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.05, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.1, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.15, 500);
    DBBenchmark::leveldbBenchmarkAMSimulationWithWriteBuffer(entries, bufferSize, 0.2, 500);
}
