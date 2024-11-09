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

    std::system_error errno_error(const char *what)
    {
        return std::system_error(errno, std::generic_category(), what);
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

                throw std::system_error();
            }
            if (n == 0)
            {
                // EOF is reached
                if (failOnEof)
                {
                    throw std::runtime_error("Unexpected EOF");
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

                throw std::system_error();
            }
            if (n == 0)
            {
                // EOF is reached
                if (failOnEof)
                {
                    throw std::runtime_error("Unexpected EOF");
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

    struct EntryHeader
    {
        keySize_t keySize;
        valueSize_t valueSize;
    } __attribute__((packed));

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
                std::cout << filename << std::endl;

                const std::regex logFileRegex("(\\d+).log");
                std::smatch match;
                if (std::regex_match(filename, match, logFileRegex))
                {
                    int nr = std::stoi(match[1].str());
                    std::cout << "log file: " << nr << std::endl;
                    logFileNumbers.push_back(nr);
                }
            }
        }

        // sort files
        std::sort(logFileNumbers.begin(), logFileNumbers.end());

        // determine next log file number
        if (logFileNumbers.size() > 0)
        {
            nextLogFileNr = logFileNumbers[logFileNumbers.size() - 1] + 1;
        }

        openCurrentLogFile();
    }

    void BitcaskDb::rotateCurrentLogFile()
    {
        // move current.log to next log file
        if (std::rename((dbPath / "current.log").c_str(), (dbPath / (std::to_string(nextLogFileNr++) + ".log")).c_str()))
        {
            throw errno_error("rename current.log");
        }

        currentOffsets.clear();

        // TODO: create index

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
        if (lseek(currentLogFile, 0, SEEK_SET) == -1)
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

            EntryHeader header;
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
        currentOffsets.clear();
    }

    void BitcaskDb::put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData)
    {
        auto offset = lseek64(currentLogFile, 0, SEEK_CUR);

        EntryHeader header = {keySize, valueSize};
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
            if (compareKey(offsets->second, keySize, keyData, vSize))
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
        auto offsets = currentOffsets.find(hash(keySize, keyData));
        for (; offsets != currentOffsets.end(); offsets++)
        {
            valueSize_t vSize;
            if (!compareKey(offsets->second, keySize, keyData, vSize))
            {
                continue;
            }

            // extract value
            std::unique_ptr<DataBuffer> buffer(new DataBuffer(vSize));
            pReadFully(currentLogFile, buffer->data, vSize, offsets->second + sizeof(EntryHeader) + keySize);
            return buffer;
        }
        return NULL;
    }

    bool BitcaskDb::compareKey(offset_t offset, keySize_t keySize, void *keyData, valueSize_t &valueSize)
    {
        EntryHeader header;
        auto bytesRead = pReadFully(currentLogFile, reinterpret_cast<char *>(&header), sizeof(header), offset);
        if (bytesRead != sizeof(header))
        {
            return false;
        }

        valueSize = header.valueSize;

        if (header.keySize != keySize)
        {
            return false;
        }

        // read key
        std::unique_ptr<uint8_t> keyFromFile(new uint8_t[keySize]);
        bytesRead = pReadFully(currentLogFile, keyFromFile, keySize, offset + sizeof(header));
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