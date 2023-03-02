#ifndef DB_INDEX_HPP
#define DB_INDEX_HPP

#include <dbRecord.hpp>

#include <string>

class DBIndex
{
public:
    virtual void insertRecord(const DBRecord& r) noexcept(true) = 0;
    virtual void deleteRecord(const std::string& key) noexcept(true) = 0;
    virtual std::vector<DBRecord> psearch(const std::string& key) noexcept(true) = 0;
    virtual std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) = 0;
    virtual std::vector<DBRecord> getAllRecords() noexcept(true) = 0;
    virtual size_t getRecordsNumber() noexcept(true) = 0;
    virtual std::string getIndexFolder() noexcept(true) = 0;

    virtual ~DBIndex() noexcept(true)
    {

    }
};

#endif