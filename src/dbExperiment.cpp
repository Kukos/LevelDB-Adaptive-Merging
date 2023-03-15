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



void DBExperiment::experiments() noexcept(true){
    // expDiffDataRange();
    // expDiffSelectivity();

    // expDiffDataSize();

  //  expDiffBatchNumberDBModification();
    
 //   expDiffQueryNumber(2);
    expDiffQueryNumber(5);
    expDiffQueryNumber(10);

}



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

void DBExperiment::expDiffDataSize() noexcept(true){

    // query number
    const size_t nmbQuery = 50;

    // data range (%) of the query
    const size_t dataRange = 40;

    // selectivity (%) of the query
    const size_t sel = 1;

    LOGGER_LOG_INFO("Starting  experiment: different data size");
    std::string folderName = std::string("./expDiffDataSize");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    log << " DataSize \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    experimentNoModification("DataSize", log, dataRange, 1000000, nmbQuery, sel);
    experimentNoModification("DataSize", log, dataRange, 5000000, nmbQuery, sel);
    experimentNoModification("DataSize", log, dataRange, 10000000, nmbQuery, sel);
    experimentNoModification("DataSize", log, dataRange, 20000000, nmbQuery, sel);
    experimentNoModification("DataSize", log, dataRange, 50000000, nmbQuery, sel);

}


void DBExperiment::expDiffSelectivity() noexcept(true){

    // database record number
    const size_t nmbRec = 10000000;

    // query number
    const size_t nmbQuery = 100;

    // data range (%) of the query
    const size_t dataRange = 40;

    LOGGER_LOG_INFO("Starting  experiment: different selectivity");
    std::string folderName = std::string("./expDiffSelectivity");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    log << " Selectivity(%) \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    experimentNoModification("Selectivity", log, dataRange, nmbRec, nmbQuery, 1);
    experimentNoModification("Selectivity", log, dataRange, nmbRec, nmbQuery, 2);
    experimentNoModification("Selectivity", log, dataRange, nmbRec, nmbQuery, 5);
    experimentNoModification("Selectivity", log, dataRange, nmbRec, nmbQuery, 10);


}


void DBExperiment::expDiffQueryNumber(size_t dataRange) noexcept(true){

    // database record number
    const size_t nmbRec = 10000000;

    // selectivity (%) of the query
    const size_t sel = 1;

    // data range (%) of the query
//    const size_t dataRange = 20;

    LOGGER_LOG_INFO("Starting  experiment: different query number");
    std::string folderName = std::string("./expDiffQueryNumberDataRange");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::to_string(dataRange) + std::string("_log.txt"); 
    

    std::ofstream log;
    log.open(logFileName.c_str());

    log << " QueryNumber \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

//    experimentNoModification("QueryNumber", log, dataRange, nmbRec, 20, sel);
//    experimentNoModification("QueryNumber", log, dataRange, nmbRec, 50, sel);
//    experimentNoModification("QueryNumber", log, dataRange, nmbRec, 100, sel);
    experimentNoModification("QueryNumber", log, dataRange, nmbRec, 200, sel);
 //   experimentNoModification("QueryNumber", log, dataRange, nmbRec, 500, sel);


    log.close();

}



void DBExperiment::expDiffDataRange() noexcept(true){

    // database record number
    const size_t nmbRec = 10000000;

    // query number
    const size_t nmbQuery = 100;

    // selectivity (%) of the query
    const size_t sel = 1;

    LOGGER_LOG_INFO("Starting  experiment: different data range");
    std::string folderName = std::string("./expDiffDataRange");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());

    log << " Data range(%) \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    experimentNoModification("DataRange", log, 2, nmbRec, nmbQuery, sel);
    experimentNoModification("DataRange", log, 5, nmbRec, nmbQuery, sel);
    experimentNoModification("DataRange", log, 20, nmbRec, nmbQuery, sel);
    experimentNoModification("DataRange", log, 50, nmbRec, nmbQuery, sel);
    experimentNoModification("DataRange", log, 100, nmbRec, nmbQuery, sel);

    log.close();

}
void DBExperiment::experimentNoModification(std::string expType, std::ofstream& log, const size_t dataRange, const size_t nmbRec, const size_t nmbQuery, const size_t sel) noexcept(true){


    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(nmbRec, 113);
    // std::vector<DBRecord> records = DBRecordGenerator::generateRecords(10 * 1000 * 1000, 113);

    // the end value of choosing must be less than n (depending on selectivity)
    // First data
    //const size_t beginRange=1;
    //const size_t endRange = (dataRange * nmbRec)/100 - sel*nmbRec/100;

// Last data
    const size_t beginRange = nmbRec - (dataRange * nmbRec)/100;
    const size_t endRange = beginRange + dataRange*nmbRec/100-1 - sel*nmbRec/100;

    std::cout << "Begin range: \t" << beginRange << "End range: \t" <<  endRange << std::endl;


    if (expType=="DataRange"){
           log << std::to_string(dataRange) <<  "\t" ;    
    }
 
    if (expType=="Selectivity"){
           log << std::to_string(sel) <<  "\t" ;    
    }

    if (expType=="QueryNumber"){
           log << std::to_string(nmbQuery) <<  "\t" ;    
    }

    if (expType=="DataSize"){
        log << std::to_string(nmbRec) <<  "\t" ;    
    }

    

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> distr(beginRange, endRange);

    std::vector<size_t> queryCount;

    for (size_t i=0; i<nmbQuery; i++){
        queryCount.push_back(distr(gen));
    }

    DBExperiment::expFullScan(records, log, queryCount, nmbRec,  sel);
    DBExperiment::expSecondaryIndexScan(records, log, queryCount, nmbRec,  sel);
    DBExperiment::expAdaptiveMerging(records, log, queryCount, nmbRec,  sel);

    log << std::endl;


}