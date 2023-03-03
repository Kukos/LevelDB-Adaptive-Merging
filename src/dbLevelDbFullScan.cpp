#include <dbLevelDbFullScan.hpp>

void DBLevelDbFullScan::do_insertRecord(const DBRecord& r) noexcept(true)
{
    // insert is like normal insert into prim Index
    primaryIndex->insertRecord(r);
}

void DBLevelDbFullScan::do_deleteRecord(const std::string& key) noexcept(true)
{
    // we need to find key in primary key for secondary key and then delete record from prim Index
    std::vector<DBRecord> entries = primaryIndex->getAllRecords();
    for (const auto& r : entries)
        if (key == r.getVal().ToString().substr(0, 8))
        {
            primaryIndex->deleteRecord(r.getKey().ToString());
            break;
        }
}

std::vector<DBRecord> DBLevelDbFullScan::do_psearch(const std::string& key) noexcept(true)
{
    // we need to find key in primary key for secondary key and then return record
    std::vector<DBRecord> entries = primaryIndex->getAllRecords();
    std::vector<DBRecord> ret;

    for (const auto& r : entries)
        if (key == r.getVal().ToString().substr(0, 8))
        {
            ret.push_back(r);
            break;
        }

    return ret;
}

std::vector<DBRecord> DBLevelDbFullScan::do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    if (maxKey < minKey)
        return std::vector<DBRecord>();

    std::vector<DBRecord> entries = primaryIndex->getAllRecords();
    std::vector<DBRecord> ret;

    for (const auto& r : entries)
    {
        const std::string secKey = r.getVal().ToString().substr(0, 8);
        if (secKey >= minKey && secKey <= maxKey)
            ret.push_back(r);
    }

    return ret;
}

std::vector<DBRecord> DBLevelDbFullScan::do_getAllRecords() noexcept(true)
{
    return primaryIndex->getAllRecords();
}

void DBLevelDbFullScan::insertRecord(const DBRecord& r) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_insertRecord(r);
}

void DBLevelDbFullScan::deleteRecord(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_deleteRecord(key);
}

std::vector<DBRecord> DBLevelDbFullScan::psearch(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_psearch(key);
}

std::vector<DBRecord> DBLevelDbFullScan::rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_rsearch(minKey, maxKey);
}

std::vector<DBRecord> DBLevelDbFullScan::getAllRecords() noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_getAllRecords();
}