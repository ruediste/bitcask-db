#include "bitcask-db.hpp"
#include "xxhash.hpp"

namespace bitcask
{
    void BitcaskDb::open(const std::filesystem::path &path)
    {
        this->dbPath = path;
        std::filesystem::create_directories(path);
        this->currentLogFile = std::fstream(path / "current.log", std::ios_base::binary);
    }

    void BitcaskDb::close()
    {
        this->currentLogFile.close();
    }

    hash_t hash(size_t size, void *data)
    {
        return xxh::xxhash<32>(data, size);
    }

    void BitcaskDb::put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData)
    {
        this->currentLogFile.seekg(0, std::ios::end);
        auto offset = this->currentLogFile.tellg();
        currentLogFile.write(reinterpret_cast<char *>(&keySize), sizeof(keySize));
        currentLogFile.write(reinterpret_cast<char *>(&valueSize), sizeof(valueSize));
        currentLogFile.write(reinterpret_cast<char *>(keyData), keySize);
        currentLogFile.write(reinterpret_cast<char *>(valueData), valueSize);
        auto h = hash(keySize, keyData);
        this->currentOffsets.try_emplace(h).first->second.push_back(offset);
    }

    bool BitcaskDb::get(keySize_t keySize, void *keyData, valueSize_t &valueSize, void *&valueData)
    {
        auto buckets = this->currentOffsets.find(hash(keySize, keyData));
        if (buckets == this->currentOffsets.end())
        {
            return false;
        }

        for (auto offset : buckets->second)
        {
            this->currentLogFile.seekg(offset);
            keySize_t kSize;
            valueSize_t vSize;
            this->currentLogFile.read(reinterpret_cast<char *>(&kSize), sizeof(kSize));
            this->currentLogFile.read(reinterpret_cast<char *>(&vSize), sizeof(vSize));
            if (kSize != keySize)
            {
                continue;
            }
            valueData = malloc(vSize);
            this->currentLogFile.read(reinterpret_cast<char *>(valueData), vSize);
            valueSize = vSize;
            return true;
        }
    }
}