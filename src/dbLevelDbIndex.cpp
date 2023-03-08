#include <dbLevelDbIndex.hpp>

#include <leveldb/write_batch.h>

void DBLevelDbIndex::do_insertRecord(const DBRecord& r) noexcept(true)
{
    // no buffering
    if (inMemoryIndexCapacity == 0)
    {
        db->Put(leveldb::WriteOptions(), r.getKey(), r.getVal());
    }
    else
    {
        inMemoryIndex->insertRecord(r);
        if (inMemoryIndex->getRecordsNumber() >= inMemoryIndexCapacity)
            do_flushInMemoryIndex();
    }
}

void DBLevelDbIndex::do_deleteRecord(const std::string& key) noexcept(true)
{
    // there is no way to check if db deleted entry, so lets assume that if key is not in buffer then entry is deleted from db
    if (inMemoryIndex->psearch(key).size() == 0) // not found in buffer, delete from index
        --entriesInLevelDb;
    else
        inMemoryIndex->deleteRecord(key);

    db->Delete(leveldb::WriteOptions(), leveldb::Slice(key));
}

std::vector<DBRecord> DBLevelDbIndex::do_psearch(const std::string& key) noexcept(true)
{
    std::vector<DBRecord> ret = inMemoryIndex->psearch(key);

    std::string val;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), leveldb::Slice(key), &val);
    if (status.ok())
        ret.push_back(DBRecord(key, val));

    return ret;
}

std::vector<DBRecord> DBLevelDbIndex::do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    if (maxKey < minKey)
        return std::vector<DBRecord>();

    std::vector<DBRecord> ret = inMemoryIndex->rsearch(minKey, maxKey);

    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    it->Seek(leveldb::Slice(minKey));

    while (it->Valid() && it->key().ToString() <= maxKey)
    {
        ret.push_back(DBRecord(it->key(), it->value()));
        it->Next();
    }

    delete it;

    return ret;
}

std::vector<DBRecord> DBLevelDbIndex::do_getAllRecords() noexcept(true)
{
    std::vector<DBRecord> ret = inMemoryIndex->getAllRecords();

    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();

    while (it->Valid())
    {
        ret.push_back(DBRecord(it->key(), it->value()));
        it->Next();
    }

    delete it;

    // std::sort(std::begin(ret), std::end(ret));

    return ret;
}

void DBLevelDbIndex::do_flushInMemoryIndex() noexcept(true)
{
    if (inMemoryIndex->getRecordsNumber() == 0)
    {
        LOGGER_LOG_DEBUG("Nothing to flush, inMemorIndex is empty");
        return;
    }

    std::unique_ptr<leveldb::WriteBatch> wb = std::make_unique<leveldb::WriteBatch>();

    LOGGER_LOG_DEBUG("Flushing {} entries from inMemoryIndex to the levelDB", inMemoryIndex->getRecordsNumber());

    std::vector<DBRecord> records = inMemoryIndex->getAllRecords();
    for (const auto& record: records)
        wb->Put(record.getKey(), record.getVal());

    leveldb::WriteOptions writeOptions;
    db->Write(writeOptions, wb.get());

    entriesInLevelDb += records.size();

    // since we have our buffer, lets flush memtable after moving buffer to the levelDB
  //  const DBRecord minKey = records[0]; // inMemoryIndex is sorted
  //  const DBRecord maxKey = records[records.size() - 1]; // inMemoryIndex is sorted
  //  leveldb::Slice minSlice = minKey.getKey();
  //  leveldb::Slice maxSlice = maxKey.getKey();
  //  db->CompactRange(&minSlice, &maxSlice);

    // reset inMemoryIndex
    inMemoryIndex = std::make_unique<DBInMemoryIndex>();
}

void DBLevelDbIndex::flushInMemoryIndex() noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_flushInMemoryIndex();
}

void DBLevelDbIndex::insertRecord(const DBRecord& r) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_insertRecord(r);
}

void DBLevelDbIndex::deleteRecord(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_deleteRecord(key);
}

std::vector<DBRecord> DBLevelDbIndex::psearch(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_psearch(key);
}

std::vector<DBRecord> DBLevelDbIndex::rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_rsearch(minKey, maxKey);
}

std::vector<DBRecord> DBLevelDbIndex::getAllRecords() noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_getAllRecords();
}