#include <dbWriteBuffer.hpp>

#include <algorithm>
#include <iterator>

#include <leveldb/write_batch.h>

void DBWriteBuffer::do_insert(const DBRecord& record) noexcept(true)
{
    recordsBuffer.push_back(record);

    if (recordsBuffer.size() >= capacity)
        do_flush();
}

void DBWriteBuffer::do_flush() noexcept(true)
{
    std::unique_ptr<leveldb::WriteBatch> wb = std::make_unique<leveldb::WriteBatch>();

    LOGGER_LOG_TRACE("Flushing {} entries from buffer to the DB", recordsBuffer.size());

    for (const auto& record: recordsBuffer)
        wb->Put(record.getKey(), record.getVal());

    leveldb::WriteOptions writeOptions;
    db->Write(writeOptions, wb.get());

    // since we have our buffer, lets flush memtable after moving buffer to the levelDB
    const DBRecord minKey = *std::min_element(std::begin(recordsBuffer), std::end(recordsBuffer));
    const DBRecord maxKey = *std::max_element(std::begin(recordsBuffer), std::end(recordsBuffer));
    leveldb::Slice minSlice = minKey.getKey();
    leveldb::Slice maxSlice = maxKey.getKey();
    db->CompactRange(&minSlice, &maxSlice);

    recordsBuffer.clear();
    recordsBuffer.reserve(capacity);
}

void DBWriteBuffer::insert(const DBRecord& record) noexcept(true)
{
    std::lock_guard<std::mutex> lock(bufferMutex);
    do_insert(record);
}

void DBWriteBuffer::insert(const leveldb::Slice& key, const leveldb::Slice& val) noexcept(true)
{
    std::lock_guard<std::mutex> lock(bufferMutex);
    do_insert(DBRecord(key, val));
}

void DBWriteBuffer::flush() noexcept(true)
{
    std::lock_guard<std::mutex> lock(bufferMutex);

    if (recordsBuffer.size() > 0)
        do_flush();
}
