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

void DBExperiment::expDiffQueryInBatchNumberDBModification() noexcept(true){

    // database record number
    const size_t nmbRec = 1000000;

    // selectivity (%) of the query
    const size_t sel = 1;

    size_t nDataRange = 50;

    // Number of batches
    size_t nBatch = 5;

    size_t nInsertInBatch = 10;
    size_t nDeleteInBatch = 5;

    LOGGER_LOG_INFO("Starting  experiment: DiffQueryInBatchNumber");
    std::string folderName = std::string("./expDiffQueryInBatchNumber");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());


    log << " QueryInBatch \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

//    experimentWithDBModification("QueryInBatch", log, nDataRange, nmbRec,  sel, nBatch, 2, nInsertInBatch, nDeleteInBatch);
    experimentWithDBModification("QueryInBatch", log, nDataRange, nmbRec,  sel, nBatch, 5, nInsertInBatch, nDeleteInBatch);
//    experimentWithDBModification("QueryInBatch", log, nDataRange, nmbRec,  sel, nBatch, 10, nInsertInBatch, nDeleteInBatch);
//    experimentWithDBModification("QueryInBatch", log, nDataRange, nmbRec,  sel, nBatch, 20, nInsertInBatch, nDeleteInBatch);

    log.close();

}


void DBExperiment::expDiffBatchNumberDBModification() noexcept(true){

    // database record number
    const size_t nmbRec = 10000000;

    // selectivity (%) of the query
    const size_t sel = 1;

    // Number of batches
    size_t nBatch = 10;
    size_t nQueryInBatch = 10;
    size_t nInsertInBatch = 10;
    size_t nDeleteInBatch = 5;

    LOGGER_LOG_INFO("Starting  experiment: DiffBatchNumber");
    std::string folderName = std::string("./expDiffBatchNumber");
    std::filesystem::remove_all(folderName);
    const std::string logFileName = folderName + std::string("_log.txt");
    std::ofstream log;
    log.open(logFileName.c_str());


    log << " Data range(%) \t  Full scan \t Sec create \t   Sec scan \t  Ad create \t Adaptive \t"<< std::endl;

    experimentWithDBModification("BatchSize", log, 10, nmbRec,  sel, nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch);
    experimentWithDBModification("BatchSize", log, 20, nmbRec,  sel, nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch);
    experimentWithDBModification("BatchSize", log, 50, nmbRec,  sel, nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch);
    experimentWithDBModification("BatchSize", log, 80, nmbRec,  sel, nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch);

    log.close();

}


void DBExperiment::experimentWithDBModification(std::string expType, std::ofstream& log, const size_t dataRange, const size_t nmbRec,  const size_t sel, 
const size_t nBatch, const size_t nQueryInBatch,
const size_t nInsertInBatch, const size_t nDeleteInBatch) noexcept(true){


    std::vector<DBRecord> records = DBRecordGenerator::generateRecords(nmbRec, 113);

    const size_t beginRange = nmbRec - (dataRange * nmbRec)/100;
    const size_t endRange = beginRange + dataRange*nmbRec/100-1 - sel*nmbRec/100;

    std::cout << "Begin range: \t" << beginRange << "End range: \t" <<  endRange << std::endl;



    if (expType=="QueryInBatch"){
           log << std::to_string(nQueryInBatch) <<  "\t" ;    
    }


    if (expType=="BatchSize"){
           log << std::to_string(nBatch) <<  "\t" ;    
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


    expFullScanDBModification(records, log, queryCount, nmbRec,  sel,
    nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch, recordsToInsert, elementsToDelete);
    expSecondaryIndexScanDBModification(records, log, queryCount, nmbRec,  sel,
    nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch, recordsToInsert, elementsToDelete);
    expAdaptiveMergingDBModification(records, log, queryCount, nmbRec,  sel,
    nBatch, nQueryInBatch, nInsertInBatch, nDeleteInBatch, recordsToInsert, elementsToDelete);

    log << std::endl;


}

void DBExperiment::expAdaptiveMergingDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true){

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

    std::cout << "ADAPTIVE MERGING ---- prepared" << std::endl;
    const auto startBatch1 = std::chrono::high_resolution_clock::now();


    for (size_t i=0; i<nBatch; i++){

        // search nQueryInBatch range queries
        {
            for (size_t j=i*nQueryInBatch; j<i*nQueryInBatch+nQueryInBatch; j++){
                size_t bRange= static_cast<size_t>(queryCount[j]);
                size_t eRange= static_cast<size_t>(queryCount[j]  + sel * nmbRec / 100);
                std::vector<DBRecord> ret =  amIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());

                for (const auto& r : ret)
                {
                    const std::string secKey = r.getVal().ToString().substr(0, 8);
                    dbIndex->psearch(secKey);
                }
             }
            std::cout << "ADAPTIVE MERGING ---- Batch " << i << "searched" << std::endl;
        }

        // inserting in batch
        {
                for (size_t j=i*nInsertInBatch; j<i*nInsertInBatch+nInsertInBatch; j++){
                    const auto& r = recordsToInsert[j];
                    dbIndex->insertRecord(r);
                    DBRecord temp(r);
                    temp.swapPrimaryKeyWithSecondaryKey();
                    recordsWithSecKey.push_back(temp);
                    amIndex->insertRecord(r);
                }  

                // sort records to make a range for searchfuther operations
                std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));   
            std::cout << "ADAPTIVE MERGING ---- Batch " << i << "inserted" << std::endl;
        }
    
        // deleting in batch

        {
                for (size_t j=i*nDeleteInBatch; j<i*nDeleteInBatch+nDeleteInBatch; j++){
                                    DBRecord recToDel = recordsWithSecKey[elementsToDelete[j]];
                    dbIndex->deleteRecord(recToDel.getVal().ToString());
                    amIndex->deleteRecord(recToDel.getKey().ToString());
                //    DBRecord temp(recToDel);
                //    temp.swapPrimaryKeyWithSecondaryKey();
                 //   dbSecondaryIndex->deleteRecord(temp.getKey().ToString());
                    recordsWithSecKey.erase(recordsWithSecKey.begin() + static_cast<unsigned>(elementsToDelete[j]));
                //    recordsWithSecKey.pop_back(temp);
                }
            std::cout << "ADAPTIVE MERGING ---- Batch " << i << "deleted" << std::endl;
        }

    }
  
    std::cout << "Size after: " <<  recordsWithSecKey.size() << std::endl;
    const auto endBatch1 = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch1 - startBatch1).count() << "\t";


}



void DBExperiment::expSecondaryIndexScanDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true){

    std::cout << "=== LEVELDB SECONDARY SCAN  DB Modification === "<< std::endl;

     constexpr size_t bufferCapacity = 100000;

    // Main LSMdb creation
    const std::string databaseFolderName = std::string("./secondaryIndex_main");
    std::filesystem::remove_all(databaseFolderName);
    DBIndex* dbIndex = new DBLevelDbIndex(databaseFolderName, bufferCapacity);

    // Secondary LSMdb creation
    const std::string databaseSecondaryFolderName = std::string("./secondaryIndex_secondary");
    std::filesystem::remove_all(databaseSecondaryFolderName);
    DBIndex* dbSecondaryIndex = new DBLevelDbIndex(databaseSecondaryFolderName, bufferCapacity);


    // insert (normal as prim key)
    for (const auto& r : generatedRecords)
    {
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



    std::cout << "Size before: " <<  recordsWithSecKey.size() << std::endl;

    const auto startBatch1 = std::chrono::high_resolution_clock::now();

    for (size_t i=0; i<nBatch; i++){

        // search nQueryInBatch range queries
        {
            for (size_t j=i*nQueryInBatch; j<i*nQueryInBatch+nQueryInBatch; j++){
                size_t bRange= static_cast<size_t>(queryCount[j]);
                size_t eRange= static_cast<size_t>(queryCount[j]  + sel * nmbRec / 100);
                std::vector<DBRecord> ret =  dbSecondaryIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());

                for (const auto& r : ret)
                {
                    const std::string secKey = r.getVal().ToString().substr(0, 8);
                    dbIndex->psearch(secKey);
                }
             }
            std::cout << "SECONDARY SCAN---- Batch " << i << "searched" << std::endl;
        }

        // inserting in batch
        {
                for (size_t j=i*nInsertInBatch; j<i*nInsertInBatch+nInsertInBatch; j++){
                    const auto& r = recordsToInsert[j];
                    dbIndex->insertRecord(r);
                    DBRecord temp(r);
                    temp.swapPrimaryKeyWithSecondaryKey();
                    recordsWithSecKey.push_back(temp);
                    dbSecondaryIndex->insertRecord(r);
                }  

                // sort records to make a range for searchfuther operations
                std::sort(std::begin(recordsWithSecKey), std::end(recordsWithSecKey));   
                std::cout << "SECONDARY SCAN---- Batch " << i << "inserted" << std::endl;
        }
    
        // deleting in batch

        {
                for (size_t j=i*nDeleteInBatch; j<i*nDeleteInBatch+nDeleteInBatch; j++){
                    DBRecord recToDel = recordsWithSecKey[elementsToDelete[j]];
                    dbIndex->deleteRecord(recToDel.getVal().ToString());
                    dbSecondaryIndex->deleteRecord(recToDel.getKey().ToString());
                //    DBRecord temp(recToDel);
                //    temp.swapPrimaryKeyWithSecondaryKey();
                 //   dbSecondaryIndex->deleteRecord(temp.getKey().ToString());
                    recordsWithSecKey.erase(recordsWithSecKey.begin() + static_cast<unsigned>(elementsToDelete[j]));
                //    recordsWithSecKey.pop_back(temp);
                }
            std::cout << "SECONDARY SCAN---- Batch " << i << "deleted" << std::endl;
        }

    }
  
    std::cout << "Size after: " <<  recordsWithSecKey.size() << std::endl;
    const auto endBatch1 = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch1 - startBatch1).count() << "\t";

    delete dbIndex;
    delete dbSecondaryIndex;


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

    std::cout << "Size before: " <<  recordsWithSecKey.size() << std::endl;

    const auto startBatch = std::chrono::high_resolution_clock::now();

    for (size_t i=0; i<nBatch; i++){

        // search nQueryInBatch range queries
        {
            for (size_t j=i*nQueryInBatch; j<i*nQueryInBatch+nQueryInBatch; j++){
                size_t bRange= static_cast<size_t>(queryCount[j]);
                size_t eRange= static_cast<size_t>(queryCount[j]  + sel * nmbRec / 100);
                dbIndex->rsearch(recordsWithSecKey[bRange].getKey().ToString(), recordsWithSecKey[eRange].getKey().ToString());
             }

        std::cout << "FULL SCAN---- Batch " << i << "searched" << std::endl;
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
        std::cout << "FULL SCAN---- Batch " << i << "inserted" << std::endl;
        }
    
        // deleting in batch

        {
                for (size_t j=i*nDeleteInBatch; j<i*nDeleteInBatch+nDeleteInBatch; j++){
                    DBRecord recToDel = recordsWithSecKey[elementsToDelete[j]];
                    dbIndex->deleteRecord(recToDel.getVal().ToString());
                //    DBRecord temp(recToDel);
                //    temp.swapPrimaryKeyWithSecondaryKey();
                    recordsWithSecKey.erase(recordsWithSecKey.begin() + static_cast<unsigned>(elementsToDelete[j]));
                //    recordsWithSecKey.pop_back(temp);
                }
        std::cout << "FULL SCAN---- Batch " << i << "deleted" << std::endl;
        }

    }
  
    std::cout << "Size after: " <<  recordsWithSecKey.size() << std::endl;
    const auto endBatch = std::chrono::high_resolution_clock::now();
    log  <<  std::chrono::duration_cast<std::chrono::milliseconds>(endBatch - startBatch).count() << "\t";

    delete dbIndex;


}


