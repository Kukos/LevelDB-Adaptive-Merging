#ifndef DB_RECORD_GENERATOR_HPP
#define DB_RECORD_GENERATOR_HPP

#include <vector>

#include <dbRecord.hpp>

class DBRecordGenerator
{
private:
    static std::string generateRandomDataWithBase64Val(uint32_t val, size_t length) noexcept(true);

public:
    static std::vector<DBRecord> generateRecords(size_t databaseEntries, size_t valueSize) noexcept(true);
    static uint32_t getValFromBase64String(const std::string& str) noexcept(true);
    static std::string generateBase64String(uint32_t val) noexcept(true);
};

#endif
