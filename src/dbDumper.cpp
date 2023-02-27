#include <dbDumper.hpp>
#include <logger.hpp>
#include <host.hpp>

#include <iostream>
#include <sstream>
#include <iomanip>
#include <regex>

const std::string DBDumper::fileFormatStr = std::string(".ldb");


std::vector<std::string> DBDumper::stringTokenize(const std::string& str, const std::string& delimiter) noexcept(true)
{
    std::vector<std::string> tokens;

    size_t pos = 0;
    size_t prev = 0;
    while ((pos = str.find(delimiter, prev)) != std::string::npos)
    {
        tokens.push_back(str.substr(prev, pos - prev));
        prev = pos + delimiter.size();
    }

    tokens.push_back(str.substr(prev));

    return tokens;
}

std::string DBDumper::getFileName(const std::string& baseFileName, const std::string& directoryPath) noexcept(true)
{
    std::stringstream ss;
    ss << std::setw(6) << std::setfill('0') << baseFileName;

    return directoryPath + hostPlatform::directorySeparator + ss.str() + fileFormatStr;
}

std::vector<std::vector<std::string>> DBDumper::getSSTableFiles(leveldb::DB* const db, const std::string& directoryPath) noexcept(true)
{
    std::string property;
    db->GetProperty("leveldb.sstables", &property);
    std::vector<std::string> propertyLines = DBDumper::stringTokenize(property, std::string("\n"));

// --- level 0 ---
//  241:3505428['++0EAA==' @ 932137 : 1 .. 'zzkPAA==' @ 939216 : 1]
//  201:3505369['++0IAA==' @ 852536 : 1 .. 'zzwEAA==' @ 848587 : 1]
//  235:3505571['++4CAA==' @ 899583 : 1 .. 'zzYBAA==' @ 909630 : 1]
//  247:3505512['++4OAA==' @ 944859 : 1 .. 'zzkDAA==' @ 967046 : 1]
//  228:3505400['++MIAA==' @ 889482 : 1 .. 'zzwIAA==' @ 891858 : 1]
// --- level 1 ---
//  199:2113137['++0BAA==' @ 778817 : 1 .. '3V4BAA==' @ 722379 : 1]

    std::vector<std::vector<std::string>> dbFilesDump;
    std::vector<std::string> levelFilesDump;
    for (const auto& line : propertyLines)
    {
        LOGGER_LOG_TRACE("Property line {}", line);
        if (line[0] == '-') // change level number
        {
            // extract number from line: --- level 1 ---
            // const std::string levelStr = std::regex_replace(line, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
            // std::stringstream sstream(levelStr);
            // sstream >> level;
            // std::cout << "level :" << level << std::endl;

            if (levelFilesDump.size() > 0)
            {
                dbFilesDump.push_back(levelFilesDump);
                levelFilesDump.clear();
            }
        }
        else // ssTable line: 235:3505571['++4CAA==' @ 899583 : 1 .. 'zzYBAA==' @ 909630 : 1] | file is 000235.ldb
        {
            const size_t fileNameStartIndex = 1; // first in the line is space: ' '
            const size_t fileNameEndIndex = line.find_first_of(':');
            if (fileNameEndIndex == std::string::npos)
                continue; // not found

            const std::string fileName =  DBDumper::getFileName(line.substr(fileNameStartIndex, fileNameEndIndex - 1), directoryPath);
            levelFilesDump.push_back(fileName);
        }
    }

    return dbFilesDump;
}

std::vector<DBRecord> DBDumper::dumpSSTable(const std::string& ssTablePath) noexcept(true)
{
    DBDumper::DBDumpFileBuffer buffer;

    leveldb::Status status = leveldb::DumpFile(leveldb::Env::Default(), ssTablePath, &buffer);
    if (!status.ok())
    {
        std::cerr << "Cannot dump ssTable: " << ssTablePath << " status: " << status.ToString() << std::endl;
        return std::vector<DBRecord>();
    }

    return buffer.getRecords();
}


leveldb::Status DBDumper::DBDumpFileBuffer::Append(const leveldb::Slice& data)
{
    //'Y+IAAA==' @ 802921 : val => 'ljkEAA==XzHHnmz2b8gc0Z3batKeTiGtl3OUZvOwSktcsO2HoWoTVOWLJOSRLGLqD02Fj8faa9UHXXAxii6XjW2sJTLnjZ6HYgLqQ5jQNtUOVY4e16t7T7L'

    const std::string line = std::string(data.ToString());

    const size_t keyFirstPos = line.find_first_of('\'') + 1;
    const size_t keyLastPos = line.substr(keyFirstPos).find_first_of('\'') - 1 + keyFirstPos;
    const std::string key = line.substr(keyFirstPos, keyLastPos - keyFirstPos + 1);

    const size_t valFirstPos = line.substr(keyLastPos + 2).find_first_of('\'') + 1 + keyLastPos + 2;
    const size_t valLastPos = line.substr(valFirstPos).find_first_of('\'') - 1 + valFirstPos;
    const std::string val = line.substr(valFirstPos, valLastPos - valFirstPos + 1);

    LOGGER_LOG_TRACE("Getting key: ({}) val: ({}) from SSTable data ({})", key, val, data.ToString());
    fileRecords.push_back(DBRecord(key, val));

    return leveldb::Status::OK();
}

leveldb::Status DBDumper::DBDumpFileBuffer::Close()
{
    return leveldb::Status::OK();
}

leveldb::Status DBDumper::DBDumpFileBuffer::Flush()
{
    return leveldb::Status::OK();
}

leveldb::Status DBDumper::DBDumpFileBuffer::Sync()
{
    return leveldb::Status::OK();
}