#pragma once
#include <cstddef>
#include <string>
#include <unordered_map>
#include <filesystem>

namespace bitcask
{
    typedef uint16_t keySize_t;
    typedef uint32_t valueSize_t;
    typedef uint32_t hash_t;
    typedef uint32_t offset_t;
    class BitcaskDb
    {
    public:
        bool open(const std::filesystem::path &dbPath);
        void put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData);
        void put(std::string key, std::string value)
        {
            this->put(key.size(), (void *)key.c_str(), value.size(), (void *)value.c_str());
        }
        bool get(keySize_t keySize, void *keyData, valueSize_t &valueSize, void *&valueData);
        bool get(const std::string &key, valueSize_t &valueSize, void *&valueData)
        {
            return this->get(key.size(), (void *)key.c_str(), valueSize, valueData);
        }

        bool get(const std::string &key, std::string &result)
        {
            valueSize_t valueSize;
            void *valueData;
            auto found = this->get(key.size(), (void *)key.c_str(), valueSize, valueData);
            if (found)
            {
                result = std::string((char *)valueData, valueSize);
            }
            return found;
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
        bool close();

        void dumpIndex();

    private:
        std::filesystem::path dbPath;
        std::unordered_multimap<hash_t, offset_t> currentOffsets;
        int currentLogFile;
        bool compareKey(offset_t offset, keySize_t keySize, void *keyData, valueSize_t &valueSize);
        void insertToCurrentIndex(bitcask::keySize_t keySize, void *keyData, offset_t offset);
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