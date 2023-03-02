#ifndef DB_INMEMORY_INDEX
#define DB_INMEMORY_INDEX

#include <dbIndex.hpp>

#include <string>
#include <map>
#include <mutex>

class DBInMemoryIndex : public DBIndex
{
private:
    std::mutex dbMutex;
    std::map<std::string, DBRecord> index;

    void do_insertRecord(const DBRecord& r) noexcept(true);
    void do_deleteRecord(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_psearch(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true);
    std::vector<DBRecord> do_getAllRecords() noexcept(true);

public:
    void insertRecord(const DBRecord& r) noexcept(true) override;
    void deleteRecord(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> psearch(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) override;
    std::vector<DBRecord> getAllRecords() noexcept(true) override;

    size_t getRecordsNumber() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return index.size();
    }

    std::string getIndexFolder() noexcept(true) override
    {
        return std::string("");
    }

    DBInMemoryIndex()
    {

    }

    virtual ~DBInMemoryIndex() = default;
    DBInMemoryIndex(const DBInMemoryIndex&) = delete;
    DBInMemoryIndex(DBInMemoryIndex&&) = delete;
    DBInMemoryIndex& operator=(const DBInMemoryIndex&) = delete;
    DBInMemoryIndex& operator=(DBInMemoryIndex&&) = delete;
};

#endif