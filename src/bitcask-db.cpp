#include "bitcask-db.hpp"
#include "xxhash.hpp"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <regex>

namespace bitcask
{
    hash_t hash(size_t size, void *data)
    {
        return xxh::xxhash<32>(data, size);
        // return 3;
    }

    cpptrace::system_error errno_error(std::string &&what)
    {
        return cpptrace::system_error(errno, std::move(what));
    }

    /**
     * Read a given amount of data from a file. If there are any errors, an exception is thrown.
     * If failOnEof is true (default), an exception is thrown if EOF is reached before the requested amount of data is read.
     * The function always reads the given number of bytes and returns this value.
     *
     * If failOnEof is false, the function returns the number of bytes read, which may be less than the requested amount.
     */
    size_t readFully(int fd, void *buf, size_t size, bool failOnEof = true)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = read(fd, ((uint8_t *)buf) + total, size - total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                throw errno_error("read");
            }
            if (n == 0)
            {
                // EOF is reached
                if (failOnEof)
                {
                    throw cpptrace::logic_error("Unexpected EOF");
                }
                else
                    return total;
            }
            total += n;
        }
        return total;
    }

    template <typename T>
    size_t readFully(int fd, std::unique_ptr<T> &buf, size_t size, bool failOnEof = true)
    {
        return readFully(fd, buf.get(), size, failOnEof);
    }

    /**
     * Read a given amount of data from a file. If there are any errors, an exception is thrown.
     * If failOnEof is true (default), an exception is thrown if EOF is reached before the requested amount of data is read.
     * The function always reads the given number of bytes and returns this value.
     *
     * If failOnEof is false, the function returns the number of bytes read, which may be less than the requested amount.
     */
    size_t pReadFully(int fd, void *buf, size_t size, offset_t offset, bool failOnEof = true)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = pread(fd, ((uint8_t *)buf) + total, size - total, offset + total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                throw errno_error("pread");
            }
            if (n == 0)
            {
                // EOF is reached
                if (failOnEof)
                {
                    throw cpptrace::logic_error("Unexpected EOF");
                }
                else
                    return total;
            }
            total += n;
        }
        return total;
    }

    template <typename T>
    size_t pReadFully(int fd, std::unique_ptr<T> &buf, size_t size, offset_t offset, bool failOnEof = true)
    {
        return pReadFully(fd, buf.get(), size, offset, failOnEof);
    }

    /** Write the given amount of data. Throw an exception if an error occurs */
    void writeFully(int fd, void *buf, size_t size)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = write(fd, ((uint8_t *)buf) + total, size - total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                throw std::runtime_error("write fully");
            }
            total += n;
        }
    }

    /** Write the given amount of data. Throw an exception if an error occurs */
    void pWriteFully(int fd, void *buf, size_t size, offset_t offset)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = pwrite(fd, ((uint8_t *)buf) + total, size - total, offset + total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                throw std::runtime_error("write fully");
            }
            total += n;
        }
    }

    struct IndexFileHeader
    {
        uint32_t buckets;
    } __attribute((packed));

    struct LogEntryHeader
    {
        keySize_t keySize;
        valueSize_t valueSize;
    } __attribute__((packed));

    BitcaskDb::Segment BitcaskDb::loadSegment(int nr)
    {
        Segment segment;
        segment.segmentNr = nr;
        segment.logFileFd = ::open(logFileName(nr).c_str(), O_RDONLY);
        if (segment.logFileFd == -1)
        {
            throw errno_error("open log file " + logFileName(nr).string());
        }

        segment.indexFileFd = ::open(indexFileName(nr).c_str(), O_RDONLY);
        if (segment.indexFileFd == -1)
        {
            throw errno_error("open index file " + indexFileName(nr).string());
        }

        IndexFileHeader header;
        pReadFully(segment.indexFileFd, &header, sizeof(IndexFileHeader), 0);
        segment.indexBucketCount = header.buckets;
        return segment;
    }

    void BitcaskDb::open(const std::filesystem::path &path)
    {
        dbPath = path;
        std::filesystem::create_directories(path);
        std::vector<int> logFileNumbers;

        // read all file names in directory
        for (const auto &entry : std::filesystem::directory_iterator(path))
        {
            if (entry.is_regular_file())
            {
                const std::string filename = entry.path().filename().string();

                const std::regex logFileRegex("(\\d+).log");
                std::smatch match;
                if (std::regex_match(filename, match, logFileRegex))
                {
                    int nr = std::stoi(match[1].str());
                    logFileNumbers.push_back(nr);
                }
            }
        }

        // sort files
        std::sort(logFileNumbers.begin(), logFileNumbers.end());

        // determine next log file number
        if (logFileNumbers.size() > 0)
        {
            nextSegmentNr = logFileNumbers[logFileNumbers.size() - 1] + 1;
        }

        // open segments files
        for (int nr : logFileNumbers)
        {
            segments.push_back(loadSegment(nr));
        }
        std::reverse(segments.begin(), segments.end());

        openCurrentLogFile();
    }

    struct AutoCloseFd
    {
        int fd;
        AutoCloseFd(int fd) : fd(fd)
        {
        }

        ~AutoCloseFd() noexcept(false)
        {

            if (fd == -1)
            {
                // the file descriptor was not opened correctly, don't try to close
                return;
            }

            if (close(fd))
            {
                throw errno_error("close fd");
            }
        }

        AutoCloseFd(const AutoCloseFd &other) = delete;
        AutoCloseFd &operator=(const AutoCloseFd other) = delete;
        operator int() const { return fd; }
    };

    void BitcaskDb::writeToIndex(int fd, int bucketCount, hash_t hash, offset_t offset)
    {
        int bucket = hash % bucketCount;

        // read bucket
        std::unique_ptr<uint8_t> bucketData(new uint8_t[bucketSize()]);
        pReadFully(fd, bucketData, bucketSize(), sizeof(IndexFileHeader) + bucket * bucketSize());

        for (int i = 0; i < offsetsPerBucket; i++)
        {
            offset_t *offsetFromBucket = reinterpret_cast<offset_t *>(bucketData.get() + 1 + i * sizeof(offset_t));
            if (*offsetFromBucket == 0)
            {
                // empty slot
                *offsetFromBucket = offset;
                pwrite(fd, bucketData.get(), bucketSize(), sizeof(IndexFileHeader) + bucket * bucketSize());
                return;
            }
        }

        // there was now free slot, throw for now
        throw std::runtime_error("no free slot in bucket");
    }

    void BitcaskDb::buildIndexFile(int segmentNr)
    {
        AutoCloseFd logFd = ::open(logFileName(segmentNr).c_str(), O_RDONLY);
        if (logFd == -1)
        {
            throw errno_error("open log");
        }
        int bucketCount = 8;
        while (true)
        {
            AutoCloseFd indexFd = ::open(indexFileName(segmentNr).c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
            if (indexFd == -1)
            {
                throw errno_error("failed to create index file");
            }

            if (posix_fallocate64(indexFd, 0, sizeof(IndexFileHeader) + bucketCount * bucketSize()))
            {
                throw errno_error("fallocate");
            }

            // write header
            IndexFileHeader header;
            header.buckets = bucketCount;
            pWriteFully(indexFd, &header, sizeof(header), 0);

            // seek log to beginning
            if (lseek64(logFd, 1, SEEK_SET) == -1)
            {
                throw errno_error("seek to beginning");
            }

            int keyCount = 0;
            while (true)
            {
                auto offset = lseek64(logFd, 0, SEEK_CUR);

                LogEntryHeader header;
                auto bytesRead = readFully(logFd, &header, sizeof(header), false);

                if (bytesRead < sizeof(header)) // EOF reached
                {
                    return;
                }

                std::unique_ptr<uint8_t> keyData(new uint8_t[header.keySize]);

                bytesRead = readFully(logFd, keyData.get(), header.keySize);
                auto pos = lseek64(logFd, header.valueSize, SEEK_CUR); // skip value

                auto h = hash(header.keySize, keyData.get());

                writeToIndex(indexFd, bucketCount, h, offset);
                keyCount++;

                if (keyCount >> bucketCount * 2)
                {
                    break;
                }
            }

            bucketCount *= 2;
        }
    }

    void BitcaskDb::rotateCurrentLogFile()
    {
        auto segmentNr = nextSegmentNr++;
        // move current.log to next log file
        if (std::rename((dbPath / "current.log").c_str(), logFileName(segmentNr).c_str()))
        {
            throw errno_error("rename current.log");
        }

        buildIndexFile(segmentNr);
        segments.push_front(loadSegment(segmentNr));

        currentOffsets.clear();
        openCurrentLogFile();
    }

    void BitcaskDb::openCurrentLogFile()
    {
        currentLogFile = ::open((dbPath / "current.log").c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (currentLogFile == -1)
        {
            throw errno_error("failed to open current.log");
        }

        // read file size
        struct stat st;
        if (fstat(currentLogFile, &st) == -1)
        {
            throw errno_error("read file size");
        }

        // build index from log file

        // Seek to beginning, skip one byte to avoid zero offsets
        if (lseek(currentLogFile, 1, SEEK_SET) == -1)
        {
            throw errno_error("seek to beginning");
        }

        while (true)
        {
            // get offset
            auto offset = lseek64(currentLogFile, 0, SEEK_CUR);
            if (offset == -1)
            {
                throw errno_error("get offset");
            }

            LogEntryHeader header;
            auto bytesRead = readFully(currentLogFile, &header, sizeof(header), false);

            if (bytesRead < sizeof(header)) // EOF or truncated file
            {
                if (lseek64(currentLogFile, offset, SEEK_SET) == -1)
                {
                    throw errno_error("seek to offset");
                }
                break;
            }

            std::unique_ptr<uint8_t> keyData(new uint8_t[header.keySize]);

            bytesRead = readFully(currentLogFile, keyData, header.keySize, false);

            if (bytesRead < header.keySize) // truncated file
            {
                if (lseek64(currentLogFile, offset, SEEK_SET) == -1)
                {
                    throw errno_error("seek to offset");
                }
                break;
            }

            auto pos = lseek64(currentLogFile, header.valueSize, SEEK_CUR); // skip value

            if (pos > st.st_size) // truncated file
            {
                if (lseek64(currentLogFile, offset, SEEK_SET) == -1)
                {
                    throw errno_error("seek to offset");
                }
                break;
            }

            insertToCurrentIndex(header.keySize, keyData.get(), offset);
        }
    }

    void BitcaskDb::close()
    {
        if (::close(currentLogFile) == -1)
        {
            throw errno_error("close current.log");
        }

        // close log files
        for (auto segment : segments)
        {
            if (::close(segment.logFileFd) == -1)
            {
                throw errno_error("close log file");
            }

            if (::close(segment.indexFileFd) == -1)
            {
                throw errno_error("close index file");
            }
        }
        segments.clear();

        currentOffsets.clear();
    }

    void BitcaskDb::put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData)
    {
        auto offset = lseek64(currentLogFile, 0, SEEK_CUR);

        LogEntryHeader header = {keySize, valueSize};
        writeFully(currentLogFile, &header, sizeof(header));
        writeFully(currentLogFile, keyData, keySize);
        writeFully(currentLogFile, valueData, valueSize);
        insertToCurrentIndex(keySize, keyData, offset);
    }

    void BitcaskDb::insertToCurrentIndex(bitcask::keySize_t keySize, void *keyData, offset_t offset)
    {
        auto h = hash(keySize, keyData);

        // find existing entry in index
        auto offsets = currentOffsets.find(hash(keySize, keyData));
        for (; offsets != currentOffsets.end(); offsets++)
        {
            valueSize_t vSize;
            if (compareKey(currentLogFile, offsets->second, keySize, keyData, vSize))
            {
                offsets->second = offset;
                return;
            }
        }

        // if there is no existing entry, insert into index
        currentOffsets.insert({h, offset});
    }

    std::unique_ptr<DataBuffer> BitcaskDb::get(keySize_t keySize, void *keyData)
    {
        auto keyHash = hash(keySize, keyData);
        // search current segment
        auto offsets = currentOffsets.find(keyHash);
        for (; offsets != currentOffsets.end(); offsets++)
        {
            valueSize_t valueSize;
            if (!compareKey(currentLogFile, offsets->second, keySize, keyData, valueSize))
            {
                continue;
            }

            // extract value
            std::unique_ptr<DataBuffer> buffer(new DataBuffer(valueSize));
            pReadFully(currentLogFile, buffer->data, valueSize, offsets->second + sizeof(LogEntryHeader) + keySize);
            return buffer;
        }

        // search older segments
        for (auto segment : segments)
        {
            int bucket = keyHash % segment.indexBucketCount;

            // read bucket
            std::unique_ptr<uint8_t> bucketData(new uint8_t[bucketSize()]);
            pReadFully(segment.indexFileFd, bucketData, bucketSize(), sizeof(IndexFileHeader) + bucket * bucketSize());

            for (int i = 0; i < offsetsPerBucket; i++)
            {
                offset_t offset = *(reinterpret_cast<offset_t *>(bucketData.get() + 1 + i * sizeof(offset_t)));
                if (offset != 0)
                {
                    valueSize_t valueSize;
                    if (!compareKey(segment.logFileFd, offset, keySize, keyData, valueSize))
                        continue;

                    // extract value
                    std::unique_ptr<DataBuffer> buffer(new DataBuffer(valueSize));
                    pReadFully(segment.logFileFd, buffer->data, valueSize, offset + sizeof(LogEntryHeader) + keySize);
                    return buffer;
                }
            }
        }
        return NULL;
    }

    bool BitcaskDb::compareKey(int fd, offset_t offset, keySize_t keySize, void *keyData, valueSize_t &valueSize)
    {
        LogEntryHeader header;
        pReadFully(fd, reinterpret_cast<char *>(&header), sizeof(header), offset);
        valueSize = header.valueSize;

        if (header.keySize != keySize)
        {
            return false;
        }

        // read key
        std::unique_ptr<uint8_t> keyFromFile(new uint8_t[keySize]);
        pReadFully(fd, keyFromFile, keySize, offset + sizeof(header));
        return memcmp(keyFromFile.get(), keyData, keySize) == 0;
    }

    void BitcaskDb::dumpIndex()
    {
        for (auto &offset : currentOffsets)
        {
            std::cout << offset.first << " " << offset.second << std::endl;
        }
    }
}