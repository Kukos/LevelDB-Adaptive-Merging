#ifndef DB_WRITE_BUFFER_HPP
#define DB_WRITE_BUFFER_HPP

#include <mutex>
#include <vector>
#include <memory>

#include <leveldb/db.h>

#include <dbRecord.hpp>
#include <logger.hpp>

class DBWriteBuffer
{
private:
    leveldb::DB* db;
    size_t capacity;
    std::vector<DBRecord> recordsBuffer;
    std::mutex bufferMutex;

    void do_insert(const DBRecord& record) noexcept(true);
    void do_flush() noexcept(true);

public:
    DBWriteBuffer(leveldb::DB* db, size_t capacity = 100 * 1000) noexcept(true)
    : db{db}, capacity{capacity}, recordsBuffer{std::vector<DBRecord>()}, bufferMutex{std::mutex()}
    {
        LOGGER_LOG_DEBUG("DBWriteBuffer created with {} capacity", capacity);

        recordsBuffer.reserve(capacity);
    }

    size_t getCapacity() const noexcept(true)
    {
        return capacity;
    }

    size_t getNumEntriesInBuffer() noexcept(true)
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        return recordsBuffer.size();
    }

    void insert(const DBRecord& record) noexcept(true);
    void insert(const leveldb::Slice& key, const leveldb::Slice& val) noexcept(true);
    void flush() noexcept(true);

    virtual ~DBWriteBuffer() noexcept(true)
    {
        LOGGER_LOG_DEBUG("DBWriteBuffer destructor current entries:{}/{}, flushing", getNumEntriesInBuffer(), capacity);

        flush();
    }

    // rule of 5 since we have custom destructor
    // buffer cannot be moved or copy only create + destroy
    DBWriteBuffer() = delete;
    DBWriteBuffer(const DBWriteBuffer&) = delete;
    DBWriteBuffer(DBWriteBuffer&&) = delete;
    DBWriteBuffer& operator=(const DBWriteBuffer&) = delete;
    DBWriteBuffer& operator=(DBWriteBuffer&&) = delete;
};

#endif
