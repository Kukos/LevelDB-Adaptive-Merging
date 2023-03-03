#ifndef DB_LEVELDB_FULL_SCAN_HPP
#define DB_LEVELDB_FULL_SCAN_HPP

#include <dbLevelDbIndex.hpp>
#include <dbIndex.hpp>
#include <logger.hpp>

#include <memory>
#include <mutex>

class DBLevelDbFullScan : public DBIndex
{
private:
    std::unique_ptr<DBIndex> primaryIndex;
    std::mutex dbMutex;

    void do_insertRecord(const DBRecord& r) noexcept(true);
    void do_deleteRecord(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_psearch(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true);
    std::vector<DBRecord> do_getAllRecords() noexcept(true);

public:
    // record is normal: key: primary key, value: secondary value which is secondaryKey|padding
    void insertRecord(const DBRecord& r) noexcept(true) override;

    // key is a secondaryKey, so its 8 first bytes from secondaryValue
    void deleteRecord(const std::string& key) noexcept(true) override;

    // key is a secondaryKey, so its 8 first bytes from secondaryValue
    std::vector<DBRecord> psearch(const std::string& key) noexcept(true) override;

    // key is a secondaryKey, so its 8 first bytes from secondaryValue
    std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) override;
    std::vector<DBRecord> getAllRecords() noexcept(true) override;

    size_t getRecordsNumber() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return primaryIndex->getRecordsNumber();
    }

    std::string getIndexFolder() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return primaryIndex->getIndexFolder();
    }

    DBLevelDbFullScan(const std::string& dbFolderPath, size_t bufferCapacity = 100 * 1000)
    : primaryIndex{std::make_unique<DBLevelDbIndex>(dbFolderPath, bufferCapacity)}
    {
        LOGGER_LOG_DEBUG("DBLevelDbFullScan created path:{}, bufferCapacity: {}", dbFolderPath, bufferCapacity);
    }

    virtual ~DBLevelDbFullScan() noexcept(true) = default;

    DBLevelDbFullScan() = delete;
    DBLevelDbFullScan(const DBLevelDbFullScan&) = delete;
    DBLevelDbFullScan(DBLevelDbFullScan&&) = delete;
    DBLevelDbFullScan& operator=(const DBLevelDbFullScan&) = delete;
    DBLevelDbFullScan& operator=(DBLevelDbFullScan&&) = delete;
};


#endif