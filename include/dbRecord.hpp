#ifndef DB_RECORD_HPP
#define DB_RECORD_HPP

#include <logger.hpp>

#include <leveldb/db.h>

#include <string>

class DBRecord
{
private:
    std::string keyData;
    std::string valData;
    leveldb::Slice key;
    leveldb::Slice val;
public:
    DBRecord(const std::string& keyString, const std::string& valString)
    : keyData{keyString}, valData{valString}, key(keyData), val(valData)
    {

    }

    DBRecord(const leveldb::Slice& key, const leveldb::Slice& val) noexcept(true)
    : keyData(key.ToString()), valData(val.ToString()), key(keyData), val(valData)
    {

    }

    leveldb::Slice getKey() const noexcept(true)
    {
        return key;
    }

    leveldb::Slice getVal() const noexcept(true)
    {
        return val;
    }

    size_t getKeySize() const noexcept(true)
    {
        return key.size();
    }

    size_t getValSize() const noexcept(true)
    {
        return val.size();
    }

    size_t getRecordSize() const noexcept(true)
    {
        return getKeySize() + getValSize();
    }

    void swapPrimaryKeyWithSecondaryKey() noexcept(true)
    {
        // we need to swap 8 first chars from val (secondary key) with key
        std::string newKey = valData.substr(0, 8);
        std::string newValue = keyData + valData.substr(8);

        LOGGER_LOG_TRACE("Swapped Record ({}, {}) into Record ({}, {})", keyData, valData, newKey, newValue);

        keyData = newKey;
        valData = newValue;
        key = leveldb::Slice(keyData);
        val = leveldb::Slice(valData);
    }

    DBRecord(const DBRecord& other) noexcept(true)
    : DBRecord(other.getKey(), other.getVal())
    {

    }

    DBRecord& operator=(const DBRecord& other) noexcept(true)
    {
        if (this == &other)
            return *this;

        keyData = other.getKey().ToString();
        valData = other.getVal().ToString();
        key = leveldb::Slice(keyData);
        val = leveldb::Slice(valData);

        return *this;
    }

    // there is no move. MOVE = COPY
    DBRecord(DBRecord&& other) noexcept(true)
    : DBRecord(other.getKey(), other.getVal())
    {

    }

    DBRecord& operator=(DBRecord&& other) noexcept(true)
    {
        if (this == &other)
            return *this;

        keyData = other.getKey().ToString();
        valData = other.getVal().ToString();
        key = leveldb::Slice(keyData);
        val = leveldb::Slice(valData);

        return *this;
    }

    DBRecord() noexcept(true)
    : DBRecord(std::string("key"), std::string("val"))
    {

    }

    virtual ~DBRecord() noexcept(true) = default;

    bool operator <(const DBRecord& r)
    {
        return keyData < r.getKey().ToString();
    }

    bool operator <=(const DBRecord& r)
    {
        return keyData <= r.getKey().ToString();
    }

    bool operator >(const DBRecord& r)
    {
        return keyData > r.getKey().ToString();
    }

    bool operator >=(const DBRecord& r)
    {
        return keyData < r.getKey().ToString();
    }

    bool operator ==(const DBRecord& r)
    {
        return keyData < r.getKey().ToString();
    }
};

#endif
