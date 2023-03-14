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



void DBExperiment::expDiffBatchNumberDBModification() noexcept(true){

    // database record number
    const size_t nmbRec = 100000;

    // query number
    const size_t nmbQuery = 100;

    // selectivity (%) of the query
    const size_t sel = 1;

    LOGGER_LOG_INFO("Starting  experiment: different data range");
    std::string folderName = std::string("./expDiffBatchNumber");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());


    log << " Data range(%) \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    experimentWithDBModification("DataRange", log, 2, nmbRec, nmbQuery, sel);

    log.close();

}

void DBExperiment::experimentWithDBModification(std::string expType, std::ofstream& log, const size_t dataRange, const size_t nmbRec, const size_t nmbQuery, const size_t sel) noexcept(true){

    // Number of batches
    size_t nBatch = 10;
    size_t nQueryInBatch = 10;
    size_t nInsertInBatch = 10;
    size_t nDeleteInBatch = 10;

    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(nmbRec, 113);

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

    // Randomly choosing the beginning of the range query
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> distr(beginRange, endRange);

    std::vector<size_t> queryCount;

    for (size_t i=0; i<nQueryInBatch*nBatch; i++){
        queryCount.push_back(distr(gen));
    }


    // Generate records to insert in all batches
    std::vector<DBRecord> recordsToInsert = DBRecordGenerator::generateRecords(nInsertInBatch*nBatch, 113);


    // Generate elements to delete in all batches 
    std::random_device rd1;
    std::mt19937 gen1(rd1());
    std::uniform_int_distribution<size_t> distr1(1, nmbRec);
    std::vector<size_t> elementsToDelete;

    for (size_t j=0; j<nBatch*nDeleteInBatch; j++){
        elementsToDelete.push_back(distr(gen1));
    }


    DBExperiment::expFullScanDBModification(records, log, queryCount, nmbRec,  sel,
    nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch, recordsToInsert, elementsToDelete);
 //   DBExperiment::expSecondaryIndexScan(records, log, queryCount, nmbRec,  sel);
 //   DBExperiment::expAdaptiveMerging(records, log, queryCount, nmbRec,  sel);

    log << std::endl;


}


void DBExperiment::expFullScanDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true){

    std::cout << "=== LEVELDB FULL SCAN  DB Modification === "<< std::endl;

     constexpr size_t bufferCapacity = 100000;

    // Main LSMdb creation
    const std::string databaseFolderName = std::string("./fullscan");
    std::filesystem::remove_all(databaseFolderName);
    DBIndex* dbIndex = new DBLevelDbFullScan(databaseFolderName, bufferCapacity);

    // insert (normal as prim key)
    for (const auto& r : generatedRecords)
    {
        dbIndex->insertRecord(r);
    }

    std::vector<DBRecord> recordsWithSecKey;
    for (const auto& r : generatedRecords)
    {
        DBRecord temp(r);
        temp.swapPrimaryKeyWithSecondaryKey();
        recordsWithSecKey.push_back(temp);
    }

    const auto startBatch = std::chrono::high_resolution_clock::now();

    for (size_t i=0; i<nBatch; i++){

        // search nQueryInBatch range queries
        {
            for (size_t j=i*nQueryInBatch; j<i*nQueryInBatch+nQueryInBatch; j++){
                size_t bRange= static_cast<size_t>(queryCount[j]);
                size_t eRange= static_cast<size_t>(queryCount[j]  + sel * nmbRec / 100);
                dbIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());
             }
        }

        // inserting in batch
        {
                for (size_t j=i*nInsertInBatch; j<i*nInsertInBatch+nInsertInBatch; j++){
                    const auto& r = recordsToInsert[j];
                    dbIndex->insertRecord(r);
                    DBRecord temp(r);
                    temp.swapPrimaryKeyWithSecondaryKey();
                    recordsWithSecKey.push_back(temp);
                }  

                // sort records to make a range for searchfuther operations
                std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));   
        }
    
        // deleting in batch

        {
                for (size_t j=i*nDeleteInBatch; j<i*nDeleteInBatch+nDeleteInBatch; j++){
                    DBRecord recToDel = recordsWithSecKey[elementsToDelete[j]];
                    dbIndex->deleteRecord(recToDel.getKey().ToString());
                    recordsWithSecKey.erase(recordsWithSecKey.begin() + static_cast<unsigned>(elementsToDelete[j]));
                }
        }

    }
  

    const auto endBatch = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << "\t";

    delete dbIndex;


}


