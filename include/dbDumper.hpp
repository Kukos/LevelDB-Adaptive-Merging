#ifndef DB_DUMPER_HPP
#define DB_DUMPER_HPP

#include <vector>
#include <string>

#include <dbRecord.hpp>

#include <leveldb/db.h>
#include <leveldb/dumpfile.h>
#include <leveldb/status.h>

class DBDumper
{
private:
    class DBDumpFileBuffer : public leveldb::WritableFile
    {
    private:
        std::vector<DBRecord> fileRecords;
    public:
        leveldb::Status Append(const leveldb::Slice& data) override;
        leveldb::Status Close() override;
        leveldb::Status Flush() override;
        leveldb::Status Sync() override;

        std::vector<DBRecord> getRecords() const noexcept(true)
        {
            return fileRecords;
        }
    };

    static const std::string fileFormatStr;

    static std::vector<std::string> stringTokenize(const std::string& str, const std::string& delimiter) noexcept(true);
    static std::string getFileName(const std::string& baseFileName, const std::string& directoryPath) noexcept(true);

public:
    // [1][2] -> 3rd file in 2nd level (level and files counted from 0)
    static std::vector<std::vector<std::string>> getSSTableFiles(leveldb::DB* db, const std::string& directoryPath) noexcept(true);
    static std::vector<DBRecord> dumpSSTable(const std::string& ssTablePath) noexcept(true);
};

#endif
