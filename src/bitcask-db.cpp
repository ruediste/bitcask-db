#include "bitcask-db.hpp"
#include "xxhash.hpp"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

namespace bitcask
{
    hash_t hash(size_t size, void *data)
    {
        return xxh::xxhash<32>(data, size);
        // return 3;
    }

    size_t readFully(int fd, char *buf, size_t size)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = read(fd, buf + total, size - total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                perror("read");
                if (total == 0)
                {
                    return -1;
                }
                return total;
            }
            if (n == 0)
            {
                return total;
            }
            total += n;
        }
        return total;
    }

    size_t writeFully(int fd, char *buf, size_t size)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = write(fd, buf + total, size - total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                perror("write");
                if (total == 0)
                {
                    return -1;
                }
                return total;
            }
            total += n;
        }
        return total;
    }

    size_t pReadFully(int fd, char *buf, size_t size, offset_t offset)
    {
        size_t total = 0;
        while (total < size)
        {
            ssize_t n = pread(fd, buf + total, size - total, offset + total);
            if (n == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                perror("pread");
                if (total == 0)
                {
                    return -1;
                }
                return total;
            }
            if (n == 0)
            {
                return total;
            }
            total += n;
        }
        return total;
    }

    struct EntryHeader
    {
        keySize_t keySize;
        valueSize_t valueSize;
    } __attribute__((packed));

    bool BitcaskDb::open(const std::filesystem::path &path)
    {
        dbPath = path;
        std::filesystem::create_directories(path);
        currentLogFile = ::open((path / "current.log").c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (currentLogFile == -1)
        {
            perror("open");
            return false;
        }

        // read file size
        struct stat st;
        if (fstat(currentLogFile, &st) == -1)
        {
            perror("fstat");
            return false;
        }

        // build index from log file
        if (lseek(currentLogFile, 0, SEEK_SET) == -1)
        {
            perror("lseek");
            return false;
        }

        while (true)
        {
            // get offset
            auto offset = lseek64(currentLogFile, 0, SEEK_CUR);
            if (offset == -1)
            {
                perror("lseek");
                return false;
            }

            EntryHeader header;
            auto bytesRead = readFully(currentLogFile, reinterpret_cast<char *>(&header), sizeof(header));

            if (bytesRead == -1) // Error
            {
                return false;
            }

            if (bytesRead < sizeof(header)) // EOF or truncated file
            {
                lseek64(currentLogFile, offset, SEEK_SET);
                break;
            }

            char *keyData = (char *)malloc(header.keySize);
            bytesRead = readFully(currentLogFile, keyData, header.keySize);
            if (bytesRead == -1)
            {
                free(keyData);
                return false;
            }

            if (bytesRead < header.keySize) // truncated file
            {
                free(keyData);
                lseek64(currentLogFile, offset, SEEK_SET);
                break;
            }

            auto pos = lseek64(currentLogFile, header.valueSize, SEEK_CUR); // skip value
            if (pos == -1)
            {
                free(keyData);
                perror("lseek");
                return false;
            }

            if (pos > st.st_size) // truncated file
            {
                free(keyData);
                lseek64(currentLogFile, offset, SEEK_SET);
                break;
            }

            insertToCurrentIndex(header.keySize, keyData, offset);
            free(keyData);
        }
        return true;
    }

    bool BitcaskDb::close()
    {
        if (::close(currentLogFile) == -1)
        {
            perror("close");
            return false;
        }
        return true;
    }

    void BitcaskDb::put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData)
    {
        auto offset = lseek64(currentLogFile, 0, SEEK_CUR);

        EntryHeader header = {keySize, valueSize};
        writeFully(currentLogFile, reinterpret_cast<char *>(&header), sizeof(header));
        writeFully(currentLogFile, reinterpret_cast<char *>(keyData), keySize);
        writeFully(currentLogFile, reinterpret_cast<char *>(valueData), valueSize);
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

    bool BitcaskDb::get(keySize_t keySize, void *keyData, valueSize_t &valueSize, void *&valueData)
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
            valueData = malloc(vSize);
            auto bytesRead = pReadFully(currentLogFile, reinterpret_cast<char *>(valueData), vSize, offsets->second + sizeof(EntryHeader) + keySize);
            if (bytesRead != vSize)
            {
                free(valueData);
                return false;
            }
            valueSize = vSize;
            return true;
        }
        return false;
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
        char *keyFromFile = (char *)malloc(keySize);
        bytesRead = pReadFully(currentLogFile, keyFromFile, keySize, offset + sizeof(header));
        if (bytesRead != keySize)
        {
            return false;
        }

        // compare keyData with the key in the file
        for (keySize_t i = 0; i < keySize; i++)
        {
            if (keyFromFile[i] != reinterpret_cast<char *>(keyData)[i])
            {
                free(keyFromFile);
                return false;
            }
        }

        free(keyFromFile);
        return true;
    }

    void BitcaskDb::dumpIndex()
    {
        for (auto &offset : currentOffsets)
        {
            std::cout << offset.first << " " << offset.second << std::endl;
        }
    }
}