#ifndef DB_ADAPTIVE_MERGING_INDEX
#define DB_ADAPTIVE_MERGING_INDEX

#include <dbIndex.hpp>
#include <dbLevelDbIndex.hpp>
#include <dbInMemoryIndex.hpp>
#include <logger.hpp>

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <filesystem>
#include <functional>

class DBAdaptiveMergingIndex : public DBIndex
{
private:
    class DBAdaptiveLog : public DBIndex
    {
    private:
        // C Plain of Data
        struct DBAdaptiveLogEntry
        {
        public:
            std::string filePath;
            std::string minKey;
            std::string maxKey;
            size_t numRecordsInFile;
            std::vector<uint8_t> touchedEntries; // can be bool, but since bool is packed has so much slower access
            bool shouldBeDeleted;

            DBAdaptiveLogEntry(const std::string& filePath, const std::string& minKey, const std::string& maxKey, size_t numRecordsInFile)
            : filePath{filePath}, minKey{minKey}, maxKey{maxKey}, numRecordsInFile{numRecordsInFile}, shouldBeDeleted{false}
            {
                touchedEntries.resize(numRecordsInFile);
                std::fill(std::begin(touchedEntries), std::end(touchedEntries), 0); // untouched

                LOGGER_LOG_DEBUG("DBAdaptiveLogEntry created, file: {}, range: ({}, {}), entries: {}", filePath, minKey, maxKey, numRecordsInFile);
            }

            virtual ~DBAdaptiveLogEntry() noexcept(true) = default;

            DBAdaptiveLogEntry() noexcept(true) = default;
            DBAdaptiveLogEntry(const DBAdaptiveLogEntry&) noexcept(true) = default;
            DBAdaptiveLogEntry(DBAdaptiveLogEntry&&) noexcept(true) = default;
            DBAdaptiveLogEntry& operator=(const DBAdaptiveLogEntry&) noexcept(true) = default;
            DBAdaptiveLogEntry& operator=(DBAdaptiveLogEntry&&) noexcept(true) = default;
        };

        std::shared_ptr<DBLevelDbIndex> primaryIndex;

        std::unique_ptr<DBInMemoryIndex> ramBuffer;
        size_t ramBufferCapacity;

        std::string alFolderPath;
        size_t alRecordsNumber;
        std::vector<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry> alFiles;
        size_t newFileId;

        void copyPrimIndexIntoAl() noexcept(true);
        void flushRamBuffer() noexcept(true);
        std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>> getALLogEntriesForRange(const std::string& minKey, const std::string& maxKey) noexcept(true);

    public:
        void insertRecord(const DBRecord& r) noexcept(true) override;
        void deleteRecord(const std::string& key) noexcept(true) override;
        std::vector<DBRecord> psearch(const std::string& key) noexcept(true) override;
        std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) override;
        std::vector<DBRecord> getAllRecords() noexcept(true) override;

        size_t getRecordsNumber() noexcept(true) override
        {
            return alRecordsNumber + ramBuffer->getRecordsNumber();
        }

        std::string getIndexFolder() noexcept(true) override
        {
            return alFolderPath;
        }

        DBAdaptiveLog(const std::shared_ptr<DBLevelDbIndex>& primaryIndex, const std::string& alFolderPath, size_t ramBufferCapacity)
        : primaryIndex{primaryIndex},
          ramBuffer{std::make_unique<DBInMemoryIndex>()},
          ramBufferCapacity{ramBufferCapacity},
          alFolderPath{alFolderPath},
          newFileId{0}
        {
            std::filesystem::create_directories(alFolderPath);

            LOGGER_LOG_DEBUG("DBAdaptiveMergingIndex::DBAdaptiveLog created path: {}, bufferCapacity: {}", alFolderPath, ramBufferCapacity);

            copyPrimIndexIntoAl();

            LOGGER_LOG_DEBUG("PrimaryIndex copied to DBAdaptiveMergingIndex::DBAdaptiveLog, ready to use");
        }

        virtual ~DBAdaptiveLog() noexcept(true)
        {
            // flush ram buffer
            flushRamBuffer();
        }

        DBAdaptiveLog() = delete;
        DBAdaptiveLog(const DBAdaptiveLog&) = delete;
        DBAdaptiveLog(DBAdaptiveLog&&) = delete;
        DBAdaptiveLog& operator=(const DBAdaptiveLog&) = delete;
        DBAdaptiveLog& operator=(DBAdaptiveLog&&) = delete;
    };


    std::mutex dbMutex;
    std::shared_ptr<DBLevelDbIndex> primaryIndex;
    std::unique_ptr<DBLevelDbIndex> secondaryIndex;
    std::unique_ptr<DBAdaptiveLog> adaptiveLog;

    void do_insertRecord(const DBRecord& r) noexcept(true);
    void do_deleteRecord(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_psearch(const std::string& key) noexcept(true);
    std::vector<DBRecord> do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true);
    std::vector<DBRecord> do_getAllRecords() noexcept(true);

public:
    void insertRecord(const DBRecord& r) noexcept(true) override;
    void deleteRecord(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> psearch(const std::string& key) noexcept(true) override;
    std::vector<DBRecord> rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true) override;
    std::vector<DBRecord> getAllRecords() noexcept(true) override;

    size_t getRecordsNumber() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return secondaryIndex->getRecordsNumber() + adaptiveLog->getRecordsNumber();
    }

    std::string getIndexFolder() noexcept(true) override
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        return primaryIndex->getIndexFolder();
    }

    DBAdaptiveMergingIndex(const std::shared_ptr<DBLevelDbIndex>& primaryIndex, size_t secIndexBufferCapacity = 100 * 1000, size_t amBufferCapacity = 1000)
    : primaryIndex{primaryIndex},
      secondaryIndex{std::make_unique<DBLevelDbIndex>(primaryIndex->getIndexFolder() + std::string("_secIndex"), secIndexBufferCapacity)},
      adaptiveLog{std::make_unique<DBAdaptiveMergingIndex::DBAdaptiveLog>(primaryIndex, primaryIndex->getIndexFolder() + std::string("_al"), amBufferCapacity)}
    {
        LOGGER_LOG_DEBUG("DBAdaptiveMergingIndex created with Index: (path: {}, entries: {}), secIndexBufferCapacity: {} amBufferCapacity: {}",
                         primaryIndex->getIndexFolder(),
                         primaryIndex->getRecordsNumber(),
                         secIndexBufferCapacity,
                         amBufferCapacity);
    }

    virtual ~DBAdaptiveMergingIndex() noexcept(true) = default;

    DBAdaptiveMergingIndex() = delete;
    DBAdaptiveMergingIndex(const DBAdaptiveMergingIndex&) = delete;
    DBAdaptiveMergingIndex(DBAdaptiveMergingIndex&&) = delete;
    DBAdaptiveMergingIndex& operator=(const DBAdaptiveMergingIndex&) = delete;
    DBAdaptiveMergingIndex& operator=(DBAdaptiveMergingIndex&&) = delete;
};

#endif