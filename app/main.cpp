#include <dbThreadPool.hpp>
#include <logger.hpp>
#include <dbBenchmark.hpp>
#include <dbInMemoryIndex.hpp>
#include <dbRecordGenerator.hpp>

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

int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_LEVEL_INFO));
    dbThreadPoolInit();

    // DBBenchmark::leveldbBenchmark();

    dbInMemoryIndexExample();

    return 0;
}
