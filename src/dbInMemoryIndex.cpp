#include <dbInMemoryIndex.hpp>
#include <logger.hpp>

#include <iterator>
#include <algorithm>

void DBInMemoryIndex::do_insertRecord(const DBRecord& r) noexcept(true)
{
    index.insert({r.getKey().ToString(), r});
}

void DBInMemoryIndex::do_deleteRecord(const std::string& key) noexcept(true)
{
    index.erase(key);
}

std::vector<DBRecord> DBInMemoryIndex::do_psearch(const std::string& key) noexcept(true)
{
    std::vector<DBRecord> ret;
    const auto r = index.find(key);

    if (r == index.end())
        return ret; // record not found, return empty vector

    ret.push_back(r->second);
    return ret;
}

std::vector<DBRecord> DBInMemoryIndex::do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    std::vector<DBRecord> ret;

    auto lowRange = index.lower_bound(minKey);
    auto upRange = index.upper_bound(maxKey);

    if (lowRange == upRange) // no value
        return ret;

    for (auto it = lowRange; it != upRange; ++it)
        ret.push_back(it->second);

    return ret;
}

std::vector<DBRecord> DBInMemoryIndex::do_getAllRecords() noexcept(true)
{
    std::vector<DBRecord> ret;
    ret.reserve(index.size());

    std::transform(std::begin(index), std::end(index), std::back_inserter(ret), [](const auto& elem) { return elem.second; });

    return ret;
}


void DBInMemoryIndex::insertRecord(const DBRecord& r) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_insertRecord(r);
}

void DBInMemoryIndex::deleteRecord(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_deleteRecord(key);
}

std::vector<DBRecord> DBInMemoryIndex::psearch(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_psearch(key);
}

std::vector<DBRecord> DBInMemoryIndex::rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_rsearch(minKey, maxKey);
}

std::vector<DBRecord> DBInMemoryIndex::getAllRecords() noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_getAllRecords();
}