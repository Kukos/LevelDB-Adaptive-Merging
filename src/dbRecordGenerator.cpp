#include <dbRecordGenerator.hpp>
#include <logger.hpp>
#include <baseEncoder.hpp>

#include <random>
#include <iostream>

std::string DBRecordGenerator::generateBase64String(const uint32_t val) noexcept(true)
{
    // 'type punning', write val as a array of bytes
    char bytes[sizeof(val)] = {0};
    std::copy(static_cast<const char*>(static_cast<const void*>(&val)),
              static_cast<const char*>(static_cast<const void*>(&val)) + sizeof(val),
              bytes);

    // without encoding, the value can has different len like val 1 --> 0x1 0x0 0x0 0x0 so the string will have only length 1
    return std::string(BaseEncoder::base64Encode(reinterpret_cast<unsigned char *>(&bytes[0]), sizeof(val)));
}

uint32_t DBRecordGenerator::getValFromBase64String(const std::string& str) noexcept(true)
{
    uint32_t val = 0;
    std::string decoded = BaseEncoder::base64Decode(str.substr(0, 8)); // first 8 chars are important (secondary key), others are just a random 'padding'
    for (size_t i = 0; i < decoded.length(); ++i)
        val += static_cast<uint32_t>(static_cast<uint8_t>(decoded[i]) << (i * 8U));

    return val;
}

std::string DBRecordGenerator::generateRandomDataWithBase64Val(const uint32_t val, const size_t length) noexcept(true)
{
    // We need 8 chars to encode secondary key in to the value
    if (length < 8)
    {
        std::cerr << "Val length should be at least 8" << std::endl;
        return std::string("");
    }

    std::random_device rd;
    std::mt19937 gen(rd());

    const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    constexpr size_t max_index = (sizeof(charset) - 2); // -1 -> last index, -2 -> last char (without '\0')

    std::uniform_int_distribution<> distr(0, max_index);

    auto randchar = [&charset = std::as_const(charset), &distr, &gen]() -> char
    {
        return charset[distr(gen)];
    };

    std::string str(length, 0);
    // first add secondary key value to the value
    std::string valStr = generateBase64String(val);

    // next add some random chars as a padding
    std::copy(valStr.begin(), valStr.end(), str.begin());
    std::generate_n(str.begin() + static_cast<long>(valStr.length()), length - valStr.length(), randchar);

    return str;
}

std::vector<DBRecord> DBRecordGenerator::generateRecords(const size_t databaseEntries, const size_t valueSize) noexcept(true)
{
    std::vector<DBRecord> records;
    std::vector<std::string> keys;
    std::vector<std::string> values;

    LOGGER_LOG_TRACE("Generating {} entries with valueSize = {} ...", databaseEntries, valueSize);

    // generate keys
    keys.reserve(databaseEntries);
    for (size_t i = 0; i < databaseEntries; ++i)
        keys.push_back(DBRecordGenerator::generateBase64String(static_cast<uint32_t>(i  + 1)));

    std::random_device rd;
    std::shuffle(keys.begin(), keys.end(), rd);

    // generate values
    values.reserve(databaseEntries);
    for (size_t i = 0; i < databaseEntries; ++i)
        values.push_back(DBRecordGenerator::generateRandomDataWithBase64Val(static_cast<uint32_t>(i + 1), valueSize));

    std::shuffle(values.begin(), values.end(), rd);

    // create records from keys and values random permutations
    records.reserve(databaseEntries);
    for (size_t i = 0; i < databaseEntries; ++i)
        records.push_back(DBRecord(leveldb::Slice(keys[i]), leveldb::Slice(values[i])));

    LOGGER_LOG_TRACE("Generated {} entries with valueSize = {}", databaseEntries, valueSize);

    return records;
}
