#include "bitcask-db.hpp"
#include "xxhash.hpp"

namespace bitcask
{
    void BitcaskDb::open(const std::filesystem::path &path)
    {
        this->dbPath = path;
        std::filesystem::create_directories(path);
        if (NULL == this->currentLogFile.open(path / "current.log", std::ios::out | std::ios::app))
        {
            std::cout << "failed to open\n";
        }
        this->currentLogFile.close();
        if (NULL == this->currentLogFile.open(path / "current.log", std::ios::in | std::ios::out | std::ios::ate))
        {
            std::cout << "failed to open\n";
        }
    }

    void BitcaskDb::close()
    {
        this->currentLogFile.close();
    }

    hash_t hash(size_t size, void *data)
    {
        // return xxh::xxhash<32>(data, size);
        return 3;
    }

    void BitcaskDb::put(keySize_t keySize, void *keyData, valueSize_t valueSize, void *valueData)
    {
        auto offset = this->currentLogFile.pubseekoff(0, std::ios::end);
        currentLogFile.sputn(reinterpret_cast<char *>(&keySize), sizeof(keySize));
        currentLogFile.sputn(reinterpret_cast<char *>(&valueSize), sizeof(valueSize));
        currentLogFile.sputn(reinterpret_cast<char *>(keyData), keySize);
        currentLogFile.sputn(reinterpret_cast<char *>(valueData), valueSize);
        auto h = hash(keySize, keyData);

        // find existing entry in index
        auto offsets = this->currentOffsets.find(hash(keySize, keyData));
        for (; offsets != this->currentOffsets.end(); offsets++)
        {
            valueSize_t vSize;
            if (compareKey(offsets->second, keySize, keyData, vSize))
            {
                offsets->second = offset;
                return;
            }
        }

        // if there is no existing entry, insert into index
        this->currentOffsets.insert({h, offset});
    }

    bool BitcaskDb::get(keySize_t keySize, void *keyData, valueSize_t &valueSize, void *&valueData)
    {
        auto offsets = this->currentOffsets.find(hash(keySize, keyData));
        for (; offsets != this->currentOffsets.end(); offsets++)
        {
            valueSize_t vSize;
            if (!compareKey(offsets->second, keySize, keyData, vSize))
            {
                continue;
            }

            // extract value
            valueData = malloc(vSize);
            this->currentLogFile.sgetn(reinterpret_cast<char *>(valueData), vSize);
            valueSize = vSize;
            return true;
        }
        return false;
    }

    bool BitcaskDb::compareKey(offset_t offset, keySize_t keySize, void *keyData, valueSize_t &valueSize)
    {
        this->currentLogFile.pubseekpos(offset);
        keySize_t kSize;
        valueSize_t vSize;
        this->currentLogFile.sgetn(reinterpret_cast<char *>(&kSize), sizeof(kSize));
        this->currentLogFile.sgetn(reinterpret_cast<char *>(&vSize), sizeof(vSize));
        if (kSize != keySize)
        {
            return false;
        }

        // compare keyData with the key in the file
        for (keySize_t i = 0; i < keySize; i++)
        {
            char c;
            this->currentLogFile.sgetn(&c, 1);
            if (c != reinterpret_cast<char *>(keyData)[i])
            {
                return false;
            }
        }
        valueSize = vSize;
        return true;
    }

    void BitcaskDb::dumpIndex()
    {
        for (auto &offset : this->currentOffsets)
        {
            std::cout << offset.first << " " << offset.second << std::endl;
        }
    }
}