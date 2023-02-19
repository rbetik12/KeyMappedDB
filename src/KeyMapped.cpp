#include "KeyMapped.hpp"
#include <Logger.hpp>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <stdlib.h>

using namespace db;

KeyMapped::KeyMapped(const fs::path& dbPath, bool overwrite)
{
    Log::Init();

    if (overwrite && fs::exists(dbPath))
    {
        assert(fs::remove(dbPath));
    }

    bool validate = false;
    if (fs::exists(dbPath))
    {
        validate = true;
    }

    dbFileOutput = std::make_shared<std::ofstream>(std::ofstream(dbPath, std::ios::app | std::ios::binary));
    dbFileInput = std::make_shared<std::ifstream>(std::ifstream(dbPath, std::ios::binary));
    assert(dbFileOutput->good());
    assert(dbFileInput->good());
    if (validate)
    {
        ReadHeader();
        assert(header.magicNumber == MAGIC_NUMBER);
    }
    else
    {
        header.magicNumber = MAGIC_NUMBER;
    }
}

KeyMapped::~KeyMapped()
{
    WriteHeader();
}

void KeyMapped::Add(std::string_view key, std::string_view value)
{
    if (key.empty() || value.empty())
    {
        KM_WARN("Attempt to write empty key \"{}\" or value \"{}\"", key.data(), value.data());
        return;
    }

    if (key.size() >= MAX_KEY_SIZE && value.size() >= MAX_VALUE_SIZE)
    {
        KM_WARN("Attempt to write too large key \"{}\" or value \"{}\"", key.data(), value.data());
        return;
    }

    KeyValue pair{};
    pair.descriptor.keySize = key.size();
    pair.descriptor.valueSize = value.size();
    memcpy(&pair.key, key.data(), key.size());
    memcpy(&pair.value, value.data(), value.size());
    Write(pair);
}

void KeyMapped::WriteHeader()
{
    dbFileOutput->write(reinterpret_cast<const char*>(&header), sizeof(header));
    dbFileOutput->flush();
}

void KeyMapped::ReadHeader()
{
    const int headerSize = sizeof(header);
    std::streampos pos = dbFileInput->tellg();
    dbFileInput->seekg(std::ios::end);
    dbFileInput->seekg(-headerSize, std::ios::end);
    dbFileInput->read(reinterpret_cast<char*>(&header), sizeof(header));
    dbFileInput->seekg(pos);
}

void KeyMapped::Write(const KeyValue& pair)
{
    assert(pair.descriptor.valueSize != 0 && pair.descriptor.keySize != 0);
    assert(pair.descriptor.keySize < MAX_KEY_SIZE && pair.descriptor.valueSize < MAX_VALUE_SIZE);
    header.size += 1;

    dbFileOutput->write(reinterpret_cast<const char*>(&pair), sizeof(pair));
    dbFileOutput->flush();
}

KeyValue KeyMapped::Read(std::string_view key)
{
    KeyValue pair{};
    size_t offset = 0;
    for (int i = 0; i < header.size; i++)
    {
        dbFileInput->seekg(offset, std::ios::beg);
        dbFileInput->read(reinterpret_cast<char*>(&pair), sizeof(pair));
        offset += sizeof(pair);

        if (std::string(key) == pair.key)
        {
            return pair;
        }
    }

    return KeyValue{};
}

std::string KeyMapped::Get(std::string_view key)
{
    auto result = Read(key);
    std::string res = result.value;

    if (res.empty())
    {
        KM_WARN("Can't find any value for key \"{}\"", key.data());
    }
    return result.value;
}
