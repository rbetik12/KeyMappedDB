#include "KeyMapped.hpp"
#include <Logger.hpp>
#include <cassert>
#include <Timer.hpp>

using namespace db;

KeyMapped::KeyMapped(const fs::path& dbPath, bool overwrite, bool debug)
{
    Log::Init();
    showDebugInfo = debug;
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

    if (!Get(key).empty())
    {
        KM_WARN("Key \"{}\" already exists", key.data());
        return;
    }

    KeyValue pair{};
    pair.descriptor.keySize = key.size();
    pair.descriptor.valueSize = value.size();
    memcpy(&pair.key, key.data(), key.size());
    memcpy(&pair.value, value.data(), value.size());
    auto offset = Write(pair);
    hashIndex[std::string(key)] = offset;
    rbTreeIndex[std::string(key)] = offset;
    DumpRbTree();
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

size_t KeyMapped::Write(const KeyValue& pair)
{
    assert(pair.descriptor.valueSize != 0 && pair.descriptor.keySize != 0);
    assert(pair.descriptor.keySize < MAX_KEY_SIZE && pair.descriptor.valueSize < MAX_VALUE_SIZE);
    header.size += 1;

    dbFileOutput->write(reinterpret_cast<const char*>(&pair), sizeof(pair));
    dbFileOutput->flush();
    return static_cast<size_t>(dbFileOutput->tellp()) - sizeof(pair);
}

KeyValue KeyMapped::Read(std::string_view key)
{
    KeyValue kv;

    kv = ReadHashIndex(key);
    if (kv.Valid())
    {
        return kv;
    }

    kv = ReadUnIndexed(key);
    return kv;
}

std::string KeyMapped::Get(std::string_view key)
{
    auto result = Read(key);
    std::string res = result.value;
    return result.value;
}

KeyValue KeyMapped::ReadUnIndexed(std::string_view key)
{
    Timer timer;
    KeyValue pair{};
    bool success = false;
    size_t offset = 0;
    for (int i = 0; i < header.size; i++)
    {
        dbFileInput->seekg(offset, std::ios::beg);
        dbFileInput->read(reinterpret_cast<char*>(&pair), sizeof(pair));
        offset += sizeof(pair);

        if (std::string(key) == pair.key)
        {
            success = true;
            break;
        }
    }

    if (showDebugInfo)
    {
        KM_TRACE("{} took {}ms", __FUNCTION__, timer.ElapsedMillis());
    }

    if (success)
    {
        return pair;
    }
    else
    {
        return {};
    }
}

KeyValue KeyMapped::ReadHashIndex(std::string_view key)
{
    KeyValue kv{};

    if (hashIndex.find(std::string(key)) == hashIndex.end())
    {
        return kv;
    }

    const auto offset = hashIndex.at(std::string(key));
    dbFileInput->seekg(offset, std::ios::beg);
    dbFileInput->read(reinterpret_cast<char*>(&kv), sizeof(kv));
    return kv;
}

void KeyMapped::DumpRbTree()
{

}
