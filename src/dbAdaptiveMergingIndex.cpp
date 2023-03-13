#include <dbAdaptiveMergingIndex.hpp>
#include <dbThreadPool.hpp>
#include <dbDumper.hpp>
#include <host.hpp>

#include <fstream>
#include <iostream>

#include <dbRecordGenerator.hpp>



std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>> DBAdaptiveMergingIndex::DBAdaptiveLog::getALLogEntriesForRange(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    if (minKey > maxKey)
        return std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>>();

    std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>> ret;
    for (auto& alFile : alFiles)
    {
        LOGGER_LOG_TRACE("Checking file {}, !{} && {} <= {} && {} >= {}", alFile.filePath, alFile.shouldBeDeleted, minKey, alFile.maxKey, maxKey, alFile.minKey);
        if (!alFile.shouldBeDeleted && minKey <= alFile.maxKey && maxKey >= alFile.minKey)
        {
            LOGGER_LOG_TRACE("Add file {} to range overlap files", alFile.filePath);
            ret.push_back(alFile);
        }
    }

    return ret;
}


void DBAdaptiveMergingIndex::DBAdaptiveLog::copyPrimIndexIntoAl() noexcept(true)
{
    // get primaryIndex ssTables
    std::vector<std::vector<std::string>> ssTables = DBDumper::getSSTableFiles(primaryIndex->getLevelDbPtr(), primaryIndex->getIndexFolder());

    const auto singleSSTableCopyF = [this](const std::string& ssTable, const std::string& outFile, size_t vecIndex) -> void
                                    {
                                        // dump SSTable to get vector of records
                                        const std::vector<DBRecord> ssTableRecords = DBDumper::dumpSSTable(ssTable);

                                        // records are in format primKey, secKey|padding
                                        // we need secondaryIndex to swap records to secKey, primKey|padding
                                        std::vector<DBRecord> outRecords;
                                        outRecords.reserve(ssTableRecords.size());
                                        for (const auto& r : ssTableRecords)
                                        {
                                            DBRecord temp(r);
                                            temp.swapPrimaryKeyWithSecondaryKey();
                                            outRecords.push_back(temp);
                                        }

                                        // now we can create a SystemInfo for new AL file
                                        DBRecord min = *std::min_element(std::begin(outRecords), std::end(outRecords));
                                        DBRecord max = *std::max_element(std::begin(outRecords), std::end(outRecords));
                                        std::string minKey = min.getKey().ToString();
                                        std::string maxKey = max.getKey().ToString();

                                        this->alFiles[vecIndex] = DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry(outFile, minKey, maxKey, outRecords.size());

                                        // write the records to the file in format: key value\nkey value\n....
                                        std::ofstream alFile;
                                        alFile.open(outFile);

                                        for (const auto& r : outRecords)
                                        {
                                            LOGGER_LOG_TRACE("Writtitng key: ({}) and val:({})", r.getKey().ToString(), r.getVal().ToString());
                                            alFile << r.getKey().ToString() << " " << r.getVal().ToString() << std::endl;
                                        }

                                        alFile.close();
                                    };

    // calculate how many files we have (we need to resize vector to get rid of the mutex in task)
    size_t numFiles = 0;
    for (const auto& levelVec : ssTables)
        numFiles += levelVec.size();

    alFiles.resize(numFiles);

    std::vector<std::future<bool>> tasks;

    // each thread will copy 1 ssTable
    for (const auto& levelVec : ssTables)
        for (const auto& file : levelVec)
        {
            LOGGER_LOG_TRACE("ALCreate: Submitting task for ssTable: {}", file);
            const std::string outFileName = alFolderPath + hostPlatform::directorySeparator + std::to_string(newFileId) + std::string(".alf");
            tasks.push_back(dbThreadPool->threadPool.submit(singleSSTableCopyF, file, outFileName, newFileId));
            ++newFileId;
        }

    // wait for tasks
    for (const auto& t : tasks)
        t.wait();

    // copied all entries, sum them up
    for (const auto& alF : alFiles)
        alRecordsNumber += alF.numRecordsInFile;
}

void DBAdaptiveMergingIndex::DBAdaptiveLog::flushRamBuffer() noexcept(true)
{
    if (ramBuffer->getRecordsNumber() == 0)
    {
        LOGGER_LOG_DEBUG("Nothing to flush, ramBuiffer is empty");
        return;
    }

    const std::string newAlFileName = alFolderPath + hostPlatform::directorySeparator + std::to_string(newFileId) + std::string(".alf");
    ++newFileId;


    LOGGER_LOG_DEBUG("Flushing {} entries from ramBuffer to the new AL file: {}", ramBuffer->getRecordsNumber(), newAlFileName);

    // those records are already sorted, so we can easly get min and max from vector
    std::vector<DBRecord> records = ramBuffer->getAllRecords();

    // write records from buffer to the new AL file
    std::ofstream alFile;
    alFile.open(newAlFileName);

    for (const auto& r : records)
        alFile << r.getKey().ToString() << " " << r.getVal().ToString() << std::endl;

    alFile.close();

    // create AL FileInfo
    const DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry fileInfo(newAlFileName,
                                                                             records[0].getKey().ToString(),
                                                                             records[records.size() - 1].getKey().ToString(),
                                                                             records.size());

    alFiles.push_back(fileInfo);

    // notice AL metadata as we inserted new values
    alRecordsNumber += records.size();

    // reset ramBuffer
    ramBuffer = std::make_unique<DBInMemoryIndex>();
}


bool DBAdaptiveMergingIndex::DBAdaptiveLog::isQueryFullInJournal(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    for (const auto& range : queryJournal)
        if (range.first <= minKey && range.second >= maxKey) // TODO: test this formula
            return true; // query is in range

    return false;
}

void DBAdaptiveMergingIndex::DBAdaptiveLog::journalReorganization() noexcept(true)
{
    // TODO: check if std::map will be faster
    std::sort(std::begin(queryJournal), std::end(queryJournal), [](const auto &left, const auto& right) { return left.first < right.first; });

    // merge ranges
}

void DBAdaptiveMergingIndex::DBAdaptiveLog::addQueryToJournal(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    queryJournal.push_back(std::make_pair(minKey, maxKey));
    journalReorganization();
}



void DBAdaptiveMergingIndex::DBAdaptiveLog::insertRecord(const DBRecord& r) noexcept(true)
{
    ramBuffer->insertRecord(r);
    if (ramBuffer->getRecordsNumber() >= ramBufferCapacity)
            flushRamBuffer();
}

void DBAdaptiveMergingIndex::DBAdaptiveLog::deleteRecord(const std::string& key) noexcept(true)
{

    if (isQueryFullInJournal(key, key))
    {
        LOGGER_LOG_DEBUG("DeleteRecord: key {} is already in index, nothing to do", key);
        return; // nothing to do
    }


    ramBuffer->deleteRecord(key);

    const std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>> alLogVec = getALLogEntriesForRange(key, key);

    const auto deleteInAlFileF =    [](const std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>& alLog, const std::string& dKey) -> void
                                    {
                                        std::ifstream alFile;
                                        alFile.open(alLog.get().filePath);

                                        std::vector<std::string> validKeys;
                                        LOGGER_LOG_TRACE("alLog {}: <{},{}> {}", alLog.get().filePath, alLog.get().minKey, alLog.get().maxKey, alLog.get().numRecordsInFile);
                                        for (size_t i = 0; i < alLog.get().numRecordsInFile; ++i)
                                        {
                                            std::string rKey;
                                            std::string rVal;
                                            alFile >> rKey >> rVal;
                                            LOGGER_LOG_TRACE("Get Key:({}) and VAL:({}), want to delete ({}), touched[{}]={}", rKey, rVal, dKey, i, alLog.get().touchedEntries[i]);
                                            if (alLog.get().touchedEntries[i] == 0)
                                            {
                                                // delete -> mark as touched
                                                if (rKey == dKey)
                                                {
                                                    LOGGER_LOG_TRACE("Deleting {} on pos {}", dKey, i);
                                                    alLog.get().touchedEntries[i] = 1;
                                                }
                                                else
                                                    validKeys.push_back(rKey);
                                            }
                                        }

                                        // update alLog
                                        if (validKeys.size() == 0)
                                            alLog.get().shouldBeDeleted = true;
                                        else
                                        {
                                            alLog.get().minKey = *std::min_element(std::begin(validKeys), std::end(validKeys));
                                            alLog.get().maxKey = *std::max_element(std::begin(validKeys), std::end(validKeys));
                                        }

                                        alFile.close();
                                    };



    // each thread will check and delete 1 alFile
    std::vector<std::future<bool>> tasks;

    for (const auto& alLog : alLogVec)
        tasks.push_back(dbThreadPool->threadPool.submit(deleteInAlFileF, alLog, key));

    // wait for tasks
    for (const auto& t : tasks)
        t.wait();

    // record has been deleted, we should skip AL in case of searching for record
    addQueryToJournal(key, key);
}

std::vector<DBRecord> DBAdaptiveMergingIndex::DBAdaptiveLog::psearch(const std::string& key) noexcept(true)
{
    return rsearch(key, key);
}

std::vector<DBRecord> DBAdaptiveMergingIndex::DBAdaptiveLog::rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    if (minKey > maxKey)
        return std::vector<DBRecord>();

    if (isQueryFullInJournal(minKey, maxKey))
    {
        LOGGER_LOG_DEBUG("RSEARCH: < {}, {} > range is already in index, nothing to do", minKey, maxKey);      
        return std::vector<DBRecord>(); // nothing to do
    }

    std::vector<DBRecord> ret = ramBuffer->rsearch(minKey, maxKey);

    const std::vector<std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>> alLogVec = getALLogEntriesForRange(minKey, maxKey);

    const auto rsearchInAlFileF =   [](const std::reference_wrapper<DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry>& alLog, const std::string& sMinKey, const std::string& sMaxKey) -> std::vector<DBRecord>
                                    {
                                        std::ifstream alFile;
                                        alFile.open(alLog.get().filePath);

                                        std::vector<std::string> validKeys;
                                        std::vector<DBRecord> queryRet;

                                        LOGGER_LOG_TRACE("alLog {}: <{},{}> {}", alLog.get().filePath, alLog.get().minKey, alLog.get().maxKey, alLog.get().numRecordsInFile);
                                        for (size_t i = 0; i < alLog.get().numRecordsInFile; ++i)
                                        {
                                            std::string rKey;
                                            std::string rVal;
                                            alFile >> rKey >> rVal;
                                            LOGGER_LOG_TRACE("Get Key:({}) and VAL:({}), looking for ({}, {}), touched[{}]={}", rKey, rVal, sMinKey, sMaxKey, i, alLog.get().touchedEntries[i]);

                                            if (alLog.get().touchedEntries[i] == 0 && rKey >= sMinKey && rKey <= sMaxKey)
                                            {
                                                queryRet.push_back(DBRecord(rKey, rVal));
                                                alLog.get().touchedEntries[i] = 1; // just now we touched this to return in search query
                                            }

                                            if (alLog.get().touchedEntries[i] == 0) // still valid
                                                validKeys.push_back(rKey);
                                        }

                                        // update alLog
                                        if (validKeys.size() == 0)
                                            alLog.get().shouldBeDeleted = true;
                                        else
                                        {
                                            alLog.get().minKey = *std::min_element(std::begin(validKeys), std::end(validKeys));
                                            alLog.get().maxKey = *std::max_element(std::begin(validKeys), std::end(validKeys));
                                        }

                                        alFile.close();

                                        return queryRet;
                                    };



    // each thread scan 1 alFile
    std::vector<std::future<std::vector<DBRecord>>> tasks;
    for (const auto& alFile : alLogVec)
        if (!alFile.get().shouldBeDeleted)
            tasks.push_back(dbThreadPool->threadPool.submit(rsearchInAlFileF, alFile, minKey, maxKey));

    std::vector<std::vector<DBRecord>> recordsFromTasks;

    // wait for tasks
    for (auto& t : tasks)
        recordsFromTasks.push_back(t.get());

    // aggregate records from alFiles into 1 big vector ret
    for (const auto& vec : recordsFromTasks)
        ret.insert(std::end(ret), std::begin(vec), std::end(vec));

    addQueryToJournal(minKey, maxKey);

    return ret;
}

std::vector<DBRecord> DBAdaptiveMergingIndex::DBAdaptiveLog::getAllRecords() noexcept(true)
{
    std::vector<DBRecord> ret = ramBuffer->getAllRecords();

    const auto scanAlFileF =    [](const DBAdaptiveMergingIndex::DBAdaptiveLog::DBAdaptiveLogEntry& alLog) -> std::vector<DBRecord>
                                {
                                    std::ifstream alFile;
                                    alFile.open(alLog.filePath);

                                    std::vector<DBRecord> alLogRecords;

                                    for (size_t i = 0; i < alLog.numRecordsInFile; ++i)
                                    {
                                        std::string rKey;
                                        std::string rVal;
                                        alFile >> rKey >> rVal;
                                        if (alLog.touchedEntries[i] == 0)
                                            alLogRecords.push_back(DBRecord(rKey, rVal));
                                    }

                                    alFile.close();

                                    return alLogRecords;
                                };

    // each thread scan 1 alFile
    std::vector<std::future<std::vector<DBRecord>>> tasks;
    for (const auto& alFile : alFiles)
        if (!alFile.shouldBeDeleted)
            tasks.push_back(dbThreadPool->threadPool.submit(scanAlFileF, alFile));

    std::vector<std::vector<DBRecord>> recordsFromTasks;

    // wait for tasks
    for (auto& t : tasks)
        recordsFromTasks.push_back(t.get());

    // aggregate records from alFiles into 1 big vector ret
    for (const auto& vec : recordsFromTasks)
        ret.insert(std::end(ret), std::begin(vec), std::end(vec));

    return ret;
}


void DBAdaptiveMergingIndex::do_insertRecord(const DBRecord& r) noexcept(true)
{
    // new record goes into AL. When will be touched by search then it will go to the secondaryIndex
    adaptiveLog->insertRecord(r);
}

void DBAdaptiveMergingIndex::do_deleteRecord(const std::string& key) noexcept(true)
{
    // records can be in AL or secIndex so delete from both of them
    // this deletion can be do in 2 threads.
    // main thread will delete from AL, second thread (future) will delete from secondaryIndex

    // run deletion on sec Index in background
    const auto delF =   [this] (const std::string& sKey) -> void
                        {
                            secondaryIndex->deleteRecord(sKey);
                        };
    std::future<bool> secIndexDeleteTask = dbThreadPool->threadPool.submit(delF, key);

    // delete from AL
    adaptiveLog->deleteRecord(key);

    // wait for background task
    secIndexDeleteTask.wait();
}

std::vector<DBRecord> DBAdaptiveMergingIndex::do_psearch(const std::string& key) noexcept(true)
{
    std::vector<DBRecord> ret;

    // record can be in secIndex or in AL
    // main thread will check AL
    // background thread(future) will check secIndex

    const auto psearchF =   [this] (const std::string& sKey) -> std::vector<DBRecord>
                            {
                                return secondaryIndex->psearch(sKey);
                            };
    std::future<std::vector<DBRecord>> secIndexPSearchTask = dbThreadPool->threadPool.submit(psearchF, key);

    const std::vector<DBRecord> retAL = adaptiveLog->psearch(key);
    const std::vector<DBRecord> retSecIndex = secIndexPSearchTask.get();

    // copy retAL and retSecIndex to ret
    ret.insert(std::end(ret), std::begin(retAL), std::end(retAL));
    ret.insert(std::end(ret), std::begin(retSecIndex), std::end(retSecIndex));

    // ret is ready, time to add touched entries from AL to secIndex
    for (const auto& r : retAL)
        secondaryIndex->insertRecord(r);

    return ret;
}

std::vector<DBRecord> DBAdaptiveMergingIndex::do_rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    if (maxKey < minKey)
        return std::vector<DBRecord>();

    std::vector<DBRecord> ret;

    // records can be in secIndex and in AL
    // main thread will check AL
    // background thread(future) will check secIndex

    const auto rsearchF =   [this] (const std::string& sMinKey, const std::string& sMaxKey) -> std::vector<DBRecord>
                            {
                                return secondaryIndex->rsearch(sMinKey, sMaxKey);
                            };

    const auto startTimerS = std::chrono::high_resolution_clock::now();

    std::future<std::vector<DBRecord>> secIndexRSearchTask = dbThreadPool->threadPool.submit(rsearchF, minKey, maxKey);

    const std::vector<DBRecord> retAL = adaptiveLog->rsearch(minKey, maxKey);
    const std::vector<DBRecord> retSecIndex = secIndexRSearchTask.get();

    // copy retAL and retSecIndex to ret
    ret.insert(std::end(ret), std::begin(retAL), std::end(retAL));
    ret.insert(std::end(ret), std::begin(retSecIndex), std::end(retSecIndex));

    const auto endTimerS = std::chrono::high_resolution_clock::now();

    const auto startTimerI = std::chrono::high_resolution_clock::now();

    // ret is ready, time to add touched entries from AL to secIndex
    for (const auto& r : retAL)
        secondaryIndex->insertRecord(r);

    const auto endTimerI = std::chrono::high_resolution_clock::now();

    LOGGER_LOG_DEBUG("AM rsearch: {} {}, took {}, get {} from AL, {} from secIndex, inserting {} entries took {}, return to user {}",
                     DBRecordGenerator::getValFromBase64String(minKey),
                     DBRecordGenerator::getValFromBase64String(maxKey),
                     std::chrono::duration_cast<std::chrono::milliseconds>(endTimerS - startTimerS).count(),
                     retAL.size(),
                     retSecIndex.size(),
                     retAL.size(),
                     std::chrono::duration_cast<std::chrono::milliseconds>(endTimerI - startTimerI).count(),
                     ret.size());

    return ret;
}

std::vector<DBRecord> DBAdaptiveMergingIndex::do_getAllRecords() noexcept(true)
{
    std::vector<DBRecord> ret;

    // records can be in secIndex and in AL
    // main thread will get entries from AL
    // background thread(future) will get entries from secIndex

    const auto getAllRecordsF = [this] () -> std::vector<DBRecord>
                                {
                                    return secondaryIndex->getAllRecords();
                                };
    std::future<std::vector<DBRecord>> secIndexGetAllRecordsTask = dbThreadPool->threadPool.submit(getAllRecordsF);

    const std::vector<DBRecord> retAL = adaptiveLog->getAllRecords();
    const std::vector<DBRecord> retSecIndex = secIndexGetAllRecordsTask.get();

    // copy retAL and retSecIndex to ret
    ret.insert(std::end(ret), std::begin(retAL), std::end(retAL));
    ret.insert(std::end(ret), std::begin(retSecIndex), std::end(retSecIndex));

    return ret;
}

void DBAdaptiveMergingIndex::insertRecord(const DBRecord& r) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_insertRecord(r);
}

void DBAdaptiveMergingIndex::deleteRecord(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    do_deleteRecord(key);
}

std::vector<DBRecord> DBAdaptiveMergingIndex::psearch(const std::string& key) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_psearch(key);
}

std::vector<DBRecord> DBAdaptiveMergingIndex::rsearch(const std::string& minKey, const std::string& maxKey) noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_rsearch(minKey, maxKey);
}

std::vector<DBRecord> DBAdaptiveMergingIndex::getAllRecords() noexcept(true)
{
    std::lock_guard<std::mutex> lock(dbMutex);
    return do_getAllRecords();
}