#ifndef DB_LEVELDB_INDEX
#define DB_LEVELDB_INDEX

#include <dbIndex.hpp>
#include <logger.hpp>
#include <dbInMemoryIndex.hpp>

#include <leveldb/db.h>

#include <string>
#include <memory>
#include <mutex>

class DBLevelDbIndex : public DBIndex
{
private:
    std::unique_ptr<DBIndex> inMemoryIndex;
    size_t inMemoryIndexCapacity;
    std::mutex dbMutex;

    std::string dbFolderPath;
    leveldb::DB* db;
    size_t entriesInLevelDb;

    void do_insertRecord(const DBRecord& r) noexcept(true);
    void do_deleteRecord(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_psearch(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true);
    std::vector<DBRecord> do_getAllRecords() noexcept(true);
    void flushInMemoryIndex() noexcept(true);
    void do_flushInMemoryIndex() noexcept(true);

public:
    void insertRecord(const DBRecord& r) noexcept(true) override;
    void deleteRecord(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> psearch(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) override;
    std::vector<DBRecord> getAllRecords() noexcept(true) override;

    size_t getRecordsNumber() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return inMemoryIndex->getRecordsNumber() + entriesInLevelDb;
    }

    std::string getIndexFolder() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return dbFolderPath;
    }

    leveldb::DB* getLevelDbPtr() noexcept(true)
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return db;
    }

    DBLevelDbIndex(const std::string& dbFolderPath, size_t bufferCapacity = 100 * 1000)
    : inMemoryIndex{std::make_unique<DBInMemoryIndex>()}, inMemoryIndexCapacity{bufferCapacity}, dbFolderPath{dbFolderPath}
    {
        //openDB
        leveldb::Options options;
        options.create_if_missing = true;

        const leveldb::Status status = leveldb::DB::Open(options, dbFolderPath, &db);

        LOGGER_LOG_DEBUG("DBLevelDbIndex created path:{}, bufferCapacity: {}", dbFolderPath, bufferCapacity);
    }

    virtual ~DBLevelDbIndex() noexcept(true)
    {
        // flush buffer before db close
        flushInMemoryIndex();

        // close db
        delete db;
    }

    // rule of 5 since we have custom destructor
    DBLevelDbIndex() = delete;
    DBLevelDbIndex(const DBLevelDbIndex&) = delete;
    DBLevelDbIndex(DBLevelDbIndex&&) = delete;
    DBLevelDbIndex& operator=(const DBLevelDbIndex&) = delete;
    DBLevelDbIndex& operator=(DBLevelDbIndex&&) = delete;
};

#endif