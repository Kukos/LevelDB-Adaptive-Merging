#ifndef DB_EXPERIMENT_HPP
#define DB_EXPERIMENT_HPP

#include <dbRecord.hpp>

class DBExperiment{

    public:

        static void experiments() noexcept(true);
        static void expDiffDataRange() noexcept(true);
        static void expDiffSelectivity() noexcept(true);
        static void expDiffQueryNumber(size_t dataRange) noexcept(true);
        static void experimentNoModification(std::string expType, std::ofstream& log, const size_t dataRange, const size_t nmbRec, const size_t nmbQuery, const size_t sel) noexcept(true);
        
      
        static void expFullScan(const std::vector<DBRecord>& records, std::ofstream& log, std::vector<size_t>& queryCount, const size_t nmbRec,  const size_t sel) noexcept(true);
        static void expSecondaryIndexScan(const std::vector<DBRecord>& records, std::ofstream& log, std::vector<size_t>& queryCount, const size_t nmbRec, const size_t sel) noexcept(true);
        static void expAdaptiveMerging(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, const size_t nmbRec,  const size_t sel) noexcept(true);
        

        static void expDiffBatchNumberDBModification() noexcept(true);

        static void experimentWithDBModification(std::string expType, std::ofstream& log, const size_t dataRange, const size_t nmbRec, const size_t sel,
        const size_t nBatch, const size_t nQueryInBatch,
        const size_t nInsertInBatch, const size_t nDeleteInBatch) noexcept(true);

        static void expFullScanDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
        const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
        const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true);


        static void expSecondaryIndexScanDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
        const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
        const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true);


        static void  expAdaptiveMergingDBModification(const std::vector<DBRecord>& generatedRecords, std::ofstream& log,  std::vector<size_t>& queryCount, 
        const size_t nmbRec,  const size_t sel, const size_t nBatch, const size_t nQueryInBatch,
        const size_t nInsertInBatch, const size_t nDeleteInBatch, std::vector<DBRecord> recordsToInsert, std::vector<size_t> elementsToDelete) noexcept(true);


    };

#endif