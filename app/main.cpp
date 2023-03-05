#include <dbThreadPool.hpp>
#include <logger.hpp>
#include <dbBenchmark.hpp>
#include <dbInMemoryIndex.hpp>
#include <dbRecordGenerator.hpp>
#include <dbLevelDbIndex.hpp>
#include <dbLevelDbFullScan.hpp>
#include <dbAdaptiveMergingIndex.hpp>

#include <filesystem>
#include <chrono>
#include <iostream>
#include <thread>

[[maybe_unused]] static void dbInMemoryIndexExample()
{
    std::cout << "=== INMEMORY INDEX EXAMPLE === "<< std::endl;
    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10, 10);

    std::cout << "GENERATED:" << std::endl;
    DBIndex* ramIndex = new DBInMemoryIndex();

    // insert
    for (const auto& r : records)
    {
        std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        ramIndex->insertRecord(r);
    }

    // sort records to make a range for search
    std::sort(std::begin(records), std::end(records));

    // get all
    {
        const std::vector<DBRecord> indexRecords = ramIndex->getAllRecords();
        std::cout << "FROM INDEX" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    // psearch not found
    {
        const std::string key = std::string("ala");
        const std::vector<DBRecord>  s = ramIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // psearch found
    {
        const std::string key = records[0].getKey().ToString();
        const std::vector<DBRecord> s = ramIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // rsearch not found
    {
        const std::string minKey = std::string("ala");
        const std::string maxKey = std::string("ala2");
        const std::vector<DBRecord> s = ramIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        }
    }

    // rsearch found
    {
        const std::string minKey = records[0].getKey().ToString();
        const std::string maxKey = records[records.size() / 2].getKey().ToString();

        std::cout << "RSEARCH: { " + minKey << " ( " << DBRecordGenerator::getValFromBase64String(minKey) <<  " ) " << " , " << maxKey << " ( " << DBRecordGenerator::getValFromBase64String(maxKey) <<  " ) " <<  " }" << std::endl;

        const std::vector<DBRecord> s = ramIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // delete all
    {
        for (const auto& r : records)
            ramIndex->deleteRecord(r.getKey().ToString());

        const std::vector<DBRecord> indexRecords = ramIndex->getAllRecords();
        std::cout << "FROM INDEX AFTER DELETE" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    delete ramIndex;

    std::cout << "============================== "<< std::endl;
}

[[maybe_unused]] static void dbLevelDbIndexExample()
{
    std::cout << "=== LEVELDB INDEX EXAMPLE === "<< std::endl;

    const std::string databaseFolderName = std::string("./leveldb_main");
    constexpr size_t bufferCapacity = 4;

    std::filesystem::remove_all(databaseFolderName);

    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10, 10);

    std::cout << "GENERATED:" << std::endl;
    DBIndex* dbIndex = new DBLevelDbIndex(databaseFolderName, bufferCapacity);

    // insert
    for (const auto& r : records)
    {
        std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }

    // sort records to make a range for search
    std::sort(std::begin(records), std::end(records));

    // get all
    {
        const std::vector<DBRecord> indexRecords = dbIndex->getAllRecords();
        std::cout << "FROM INDEX" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    // psearch not found
    {
        const std::string key = std::string("ala");
        const std::vector<DBRecord>  s = dbIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // psearch found
    {
        const std::string key = records[records.size() / 2].getKey().ToString();
        const std::vector<DBRecord>  s = dbIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // rsearch not found
    {
        const std::string minKey = std::string("ala");
        const std::string maxKey = std::string("ala2");
        const std::vector<DBRecord> s = dbIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        }
    }

    // rsearch found
    {
        const std::string minKey = records[0].getKey().ToString();
        const std::string maxKey = records[records.size() / 2].getKey().ToString();

        std::cout << "RSEARCH: { " + minKey << " ( " << DBRecordGenerator::getValFromBase64String(minKey) <<  " ) " << " , " << maxKey << " ( " << DBRecordGenerator::getValFromBase64String(maxKey) <<  " ) " <<  " }" << std::endl;

        const std::vector<DBRecord> s = dbIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }


    // delete all
    {
        for (const auto& r : records)
            dbIndex->deleteRecord(r.getKey().ToString());

        const std::vector<DBRecord> indexRecords = dbIndex->getAllRecords();
        std::cout << "FROM INDEX AFTER DELETE" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    delete dbIndex;

    std::cout << "============================== "<< std::endl;
}

[[maybe_unused]] static void dbLevelDbFullScanExample()
{
    std::cout << "=== LEVELDB FULL SCAN EXAMPLE === "<< std::endl;

    const std::string databaseFolderName = std::string("./fullscan_main");
    constexpr size_t bufferCapacity = 4;

    std::filesystem::remove_all(databaseFolderName);

    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10, 10);

    std::cout << "GENERATED:" << std::endl;
    DBIndex* dbIndex = new DBLevelDbFullScan(databaseFolderName, bufferCapacity);

    // insert (normal as prim key)
    for (const auto& r : records)
    {
        std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }

    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : records)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }



    // sort records to make a range for search
    std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));

    // get all
    {
        const std::vector<DBRecord> indexRecords = dbIndex->getAllRecords();
        std::cout << "FROM INDEX" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    // psearch not found
    {
        const std::string key = std::string("ala");
        const std::vector<DBRecord>  s = dbIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // psearch found
    {
        const std::string key = recordsWithSecKey[recordsWithSecKey.size() / 2].getKey().ToString();
        const std::vector<DBRecord>  s = dbIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // rsearch not found
    {
        const std::string minKey = std::string("ala");
        const std::string maxKey = std::string("ala2");
        const std::vector<DBRecord> s = dbIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getVal().ToString().substr(0, 8)) <<  " ) " << " }" << std::endl;
        }
    }

    // rsearch found
    {
        const std::string minKey = recordsWithSecKey[0].getKey().ToString();
        const std::string maxKey = recordsWithSecKey[recordsWithSecKey.size() / 2].getKey().ToString();

        std::cout << "RSEARCH: { " + minKey << " ( " << DBRecordGenerator::getValFromBase64String(minKey) <<  " ) " << " , " << maxKey << " ( " << DBRecordGenerator::getValFromBase64String(maxKey) <<  " ) " <<  " }" << std::endl;

        const std::vector<DBRecord> s = dbIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getVal().ToString().substr(0, 8)) <<  " ) " << " }" << std::endl;

        }
    }


    // delete all
    {
        for (const auto& r : recordsWithSecKey)
            dbIndex->deleteRecord(r.getKey().ToString());

        const std::vector<DBRecord> indexRecords = dbIndex->getAllRecords();
        std::cout << "FROM INDEX AFTER DELETE" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    delete dbIndex;

    std::cout << "============================== "<< std::endl;
}

[[maybe_unused]] static void dbAdaptiveMergingExample()
{
    std::cout << "=== ADAPTIVE MERGING EXAMPLE === "<< std::endl;

    const std::string databaseFolderName = std::string("./adaptivemerging_main");
    constexpr size_t bufferCapacity = 4;
    constexpr size_t amBufferCapacity = 3;

    // constexpr size_t bufferCapacity = 100000;
    // constexpr size_t amBufferCapacity = 10000;

    std::filesystem::remove_all(databaseFolderName);
    std::filesystem::remove_all(databaseFolderName + std::string("_secIndex"));
    std::filesystem::remove_all(databaseFolderName + std::string("_al"));

    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10, 10);
    // std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10 * 1000 * 1000, 113);

    std::cout << "GENERATED:" << std::endl;
    std::shared_ptr<DBLevelDbIndex> dbIndex = std::make_shared<DBLevelDbIndex>(databaseFolderName, bufferCapacity);

    // insert (normal as prim key)
    for (const auto& r : records)
    {
        std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }

    // compact + delete (close) + open + wait for "repair" if needed
    dbIndex->getLevelDbPtr()->CompactRange(nullptr, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    dbIndex.reset();
    dbIndex = std::make_shared<DBLevelDbIndex>(databaseFolderName, bufferCapacity);

    std::this_thread::sleep_for(std::chrono::seconds(2));


    std::cout << "INSERTED:" << std::endl;

    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : records)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }

    // sort records to make a range for search
    std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));

    DBIndex* amIndex = new DBAdaptiveMergingIndex(dbIndex, bufferCapacity, amBufferCapacity);

    // insert directly to AM
    for (const auto& r : recordsWithSecKey)
    {
        std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        amIndex->insertRecord(r);
    }

    // get all
    {
        const std::vector<DBRecord> indexRecords = amIndex->getAllRecords();
        std::cout << "FROM INDEX" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    // psearch not found
    {
        const std::string key = std::string("ala");
        const std::vector<DBRecord>  s = amIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // psearch found
    {
        const std::string key = recordsWithSecKey[recordsWithSecKey.size() / 2].getKey().ToString();
        const std::vector<DBRecord>  s = amIndex->psearch(key);
        if (s.size() == 0)
            std::cout << "not found: " << key << std::endl;
        else
        {
            std::cout << "found: " << key << std::endl;
             for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;

        }
    }

    // rsearch not found
    {
        const std::string minKey = std::string("ala");
        const std::string maxKey = std::string("ala2");
        const std::vector<DBRecord> s = amIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getVal().ToString().substr(0, 8)) <<  " ) " << " }" << std::endl;
        }
    }

    // rsearch found
    {
        const std::string minKey = recordsWithSecKey[0].getKey().ToString();
        const std::string maxKey = recordsWithSecKey[recordsWithSecKey.size() / 2].getKey().ToString();

        std::cout << "RSEARCH: { " + minKey << " ( " << DBRecordGenerator::getValFromBase64String(minKey) <<  " ) " << " , " << maxKey << " ( " << DBRecordGenerator::getValFromBase64String(maxKey) <<  " ) " <<  " }" << std::endl;

        const std::vector<DBRecord> s = amIndex->rsearch(minKey, maxKey);
        if (s.size() == 0)
            std::cout << "not found: ( " << minKey << " , " << maxKey << " )" << std::endl;
        else
        {
            std::cout << "found: ( " << minKey << " , " << maxKey << " )" << std::endl;
            for (const auto& r : s)
                std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getVal().ToString().substr(0, 8)) <<  " ) " << " }" << std::endl;
        }
    }


    // delete all
    {
        for (const auto& r : recordsWithSecKey)
            amIndex->deleteRecord(r.getKey().ToString());

        const std::vector<DBRecord> indexRecords = amIndex->getAllRecords();
        std::cout << "FROM INDEX AFTER DELETE" << std::endl;
        for (const auto& r : indexRecords)
            std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
    }

    delete amIndex;
    std::cout << "============================== "<< std::endl;
}

int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_LEVEL_DEBUG));
    dbThreadPoolInit();

    // DBBenchmark::leveldbBenchmark();

    // dbInMemoryIndexExample();
    // dbLevelDbIndexExample();
    // dbLevelDbFullScanExample();
    dbAdaptiveMergingExample();

    return 0;
}
