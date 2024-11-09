#pragma once
#include <cstddef>
#include <string>
#include <unordered_map>
#include <filesystem>
#include <vector>

namespace bitcask
{
    typedef uint16_t keySize_t;
    typedef uint32_t valueSize_t;
    typedef uint32_t hash_t;
    typedef uint32_t offset_t;

    struct DataBuffer
    {
        size_t size;
        void *data;

        DataBuffer(size_t size)
        {
            this->size = size;
            this->data = malloc(size);
        }

        ~DataBuffer()
        {
            free(data);
        }
    };

    class BitcaskDb
    {
    public:
        void open(const std::filesystem::path &dbPath);
        void put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData);
        void put(std::string key, std::string value)
        {
            this->put(key.size(), (void *)key.c_str(), value.size(), (void *)value.c_str());
        }
        std::unique_ptr<DataBuffer> get(keySize_t keySize, void *keyData);
        std::unique_ptr<DataBuffer> get(const std::string &key)
        {
            return this->get(key.size(), (void *)key.c_str());
        }

        bool get(const std::string &key, std::string &result)
        {
            auto found = this->get(key.size(), (void *)key.c_str());
            if (found)
            {
                result = std::string((char *)found->data, found->size);
                return true;
            }
            return false;
        }

        std::string getString(const std::string &key)
        {
            std::string result;
            if (!this->get(key, result))
            {
                throw std::runtime_error("Key not found");
            }
            return result;
        }

        void remove(keySize_t keySize, void *keyData);
        void close();

        void dumpIndex();

        void rotateCurrentLogFile();

    private:
        std::filesystem::path dbPath;
        std::unordered_multimap<hash_t, offset_t> currentOffsets;
        int currentLogFile;
        bool compareKey(offset_t offset, keySize_t keySize, void *keyData, valueSize_t &valueSize);
        void insertToCurrentIndex(bitcask::keySize_t keySize, void *keyData, offset_t offset);

        /** File descriptors for the log files, without the current log file */
        std::vector<int> logFiles;
        int nextLogFileNr = 0;

        void openCurrentLogFile();
    };

    struct BitcaskKey
    {
        keySize_t size;
        void *data;
    };
    struct BitcaskValue
    {
        valueSize_t size;
        void *data;
    };
}