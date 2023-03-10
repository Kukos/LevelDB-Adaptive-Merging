#include <dbExperiment.hpp>

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
#include <fstream>
#include <random>


void DBExperiment::expSecondaryIndexScan(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, const size_t nmbRec, const size_t sel) noexcept(true){

    std::cout << "=== LEVELDB INDEX === "<< std::endl;

   constexpr size_t bufferCapacity = 100000;

    // Main LSMdb creation
    const std::string databaseFolderName = std::string("./secondaryIndex_main");
    std::filesystem::remove_all(databaseFolderName);
    DBIndex* dbIndex = new DBLevelDbIndex(databaseFolderName, bufferCapacity);

    // Secondary LSMdb creation
    const std::string databaseSecondaryFolderName = std::string("./secondaryIndex_secondary");
    std::filesystem::remove_all(databaseSecondaryFolderName);
    DBIndex* dbSecondaryIndex = new DBLevelDbIndex(databaseSecondaryFolderName, bufferCapacity);

    // insert into database
    for (const auto& r : generatedRecords)
    {
        // std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }


    const auto startBatch = std::chrono::high_resolution_clock::now();
    const auto pRecords = dbIndex->getAllRecords();

    // Key value swapping
    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : pRecords)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }

    // inserting into the Secondary LSMdb
    for (const auto& r : recordsWithSecKey)
    {
        dbSecondaryIndex->insertRecord(r);
    }

   const auto endBatch = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count()  << "\t";

    // sort records to make a range for search
    std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));

    const auto startBatch1 = std::chrono::high_resolution_clock::now();
    // Executing range queries
    for (size_t i=0; i<queryCount.size(); i++){


    size_t bRange= static_cast<size_t>(queryCount[i]);
    size_t eRange= static_cast<size_t>(queryCount[i]  + sel * nmbRec / 100);
       
       std::vector<DBRecord> ret =  dbSecondaryIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());

        for (const auto& r : ret)
        {
            const std::string secKey = r.getVal().ToString().substr(0, 8);
            dbIndex->psearch(secKey);
        }
    }


    const auto endBatch1 = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch1 - startBatch1).count()  << "\t";

    delete dbIndex;
    delete dbSecondaryIndex;
}

void DBExperiment::expFullScan(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, const size_t nmbRec,  const size_t sel) noexcept(true){

    std::cout << "=== LEVELDB FULL SCAN  === "<< std::endl;
    //constexpr size_t bufferCapacity = 1000;

     constexpr size_t bufferCapacity = 100000;

    // Main LSMdb creation
    const std::string databaseFolderName = std::string("./fullscan");
    std::filesystem::remove_all(databaseFolderName);
    DBIndex* dbIndex = new DBLevelDbFullScan(databaseFolderName, bufferCapacity);

    // insert (normal as prim key)
    for (const auto& r : generatedRecords)
    {
        // std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }



    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : generatedRecords)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }

    // sort records to make a range for search
    std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));

    const auto startBatch = std::chrono::high_resolution_clock::now();

    // Executing range queries
    for (size_t i=0; i<queryCount.size(); i++){
      size_t bRange= static_cast<size_t>(queryCount[i]);
    size_t eRange= static_cast<size_t>(queryCount[i]  + sel * nmbRec / 100);
        dbIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());
    }

    const auto endBatch = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << "\t";

    delete dbIndex;


}

void DBExperiment::expAdaptiveMerging(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, const size_t nmbRec,  const size_t sel) noexcept(true){

std::cout << "=== ADAPTIVE MERGING === "<< std::endl;

    //constexpr size_t bufferCapacity = 1000;
    //constexpr size_t amBufferCapacity = 10000;

     constexpr size_t bufferCapacity = 100000;
     constexpr size_t amBufferCapacity = 10000;

    const std::string databaseFolderName = std::string("./adaptiveMerging");
    std::filesystem::remove_all(databaseFolderName);

   std::filesystem::remove_all(databaseFolderName + std::string("_secIndex"));
   std::filesystem::remove_all(databaseFolderName + std::string("_al"));

    std::shared_ptr<DBLevelDbIndex> dbIndex = std::make_shared<DBLevelDbIndex>(databaseFolderName, bufferCapacity);

    // insert (normal as prim key)
    for (const auto& r : generatedRecords)
    {
        // std::cout << "{ " + r.getKey().ToString() << " ( " << DBRecordGenerator::getValFromBase64String(r.getKey().ToString()) <<  " ) " << " , " << r.getVal().ToString() << " }" << std::endl;
        dbIndex->insertRecord(r);
    }

    // compact + delete (close) + open + wait for "repair" if needed
    dbIndex->getLevelDbPtr()->CompactRange(nullptr, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    dbIndex.reset();
    dbIndex = std::make_shared<DBLevelDbIndex>(databaseFolderName, bufferCapacity);

    std::this_thread::sleep_for(std::chrono::seconds(2));



    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : generatedRecords)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }

     // sort records to make a range for search
    std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));

    const auto startBatch = std::chrono::high_resolution_clock::now();
    DBIndex* amIndex = new DBAdaptiveMergingIndex(dbIndex, bufferCapacity, amBufferCapacity);

    const auto endBatch = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << "\t";


    const auto startBatch1 = std::chrono::high_resolution_clock::now();
       // Executing range queries
    for (size_t i=0; i<queryCount.size(); i++){
        size_t bRange= static_cast<size_t>(queryCount[i]);
        size_t eRange= static_cast<size_t>(queryCount[i]  + sel * nmbRec / 100);
        std::vector<DBRecord> ret =  amIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());

        for (const auto& r : ret)
        {
            const std::string secKey = r.getVal().ToString().substr(0, 8);
            dbIndex->psearch(secKey);
        }
    }

   const auto endBatch1 = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch1 - startBatch1).count() << "\t";

    delete amIndex;
}


void DBExperiment::experimentNoModification() noexcept(true){


    LOGGER_LOG_INFO("Starting  experiments");
    std::string folderName = std::string("./expResults");

    std::filesystem::remove_all(folderName);

    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());


    log << " Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    // database record number
    const size_t nmbRec = 10000000;

    // range number
    const size_t nmbQuery = 100;

    // selectivity (%) of the query
    const size_t sel = 1;

    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(nmbRec, 113);
    // std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10 * 1000 * 1000, 113);

    // the end value of choosing must be less than n (depending on selectivity)
    const size_t endRange = nmbRec - sel*nmbRec/100;

    std::cout << endRange << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> distr(1, endRange);

    std::vector<size_t> queryCount;

    for (size_t i=0; i<nmbQuery; i++){
        queryCount.push_back(distr(gen));
    }

/*for (size_t i=0; i<nmbQuery; i++){
    size_t bRange= static_cast<size_t>(queryCount[i]);
    size_t eRange= static_cast<size_t>(queryCount[i]  + sel * nmbRec / 100);

    std::cout << bRange << "\t" << eRange << std::endl;
}*/

//    log << std::to_string(sel) << "\t";

    DBExperiment::expFullScan(records, log, queryCount, nmbRec,  sel);
      DBExperiment::expSecondaryIndexScan(records, log, queryCount, nmbRec,  sel);
    DBExperiment::expAdaptiveMerging(records, log, queryCount, nmbRec,  sel);

    log << std::endl;

    // std::cout << "============================== "<< std::endl;
}