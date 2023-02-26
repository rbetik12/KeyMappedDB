#include "KeyMapped.hpp"
#include <Logger.hpp>
#include <cassert>
#include <Timer.hpp>
#include <Utils.hpp>
#include "index/HashIndex.hpp"
#include "index/SlowIndex.hpp"
#include "index/SsTableIndex.hpp"

using namespace db;

KeyMapped::KeyMapped(const fs::path& dbPath, bool overwrite, bool debug, index::Type indexType) : dbPath(std::move(dbPath))
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

    const std::string headerFilePath = dbPath.stem().generic_string();
    const std::string dbFilePathWExt = headerFilePath + ".meta";
    headerPath = dbFilePathWExt;

    dbFile = std::fstream(dbPath, std::ios::in | std::ios::app | std::ios::binary);
    assert(dbFile.is_open());
    assert(dbFile.good());

    if (validate)
    {
        ReadHeader();
        assert(header.magicNumber == MAGIC_NUMBER);
    } else
    {
        header.magicNumber = MAGIC_NUMBER;
    }

    switch (indexType)
    {
        case index::Type::Hash:
            indexInstance = std::make_shared<index::HashIndex>(headerFilePath);
            break;
        case index::Type::Slow:
            indexInstance = std::make_shared<index::SlowIndex>(headerFilePath);
            break;
        case index::Type::SSTable:
            indexInstance = std::make_shared<index::SSTableIndex>(headerFilePath);
            break;
        default:
            assert(false);
    }

    indexInstance->SetWriter([&](const KeyValue& pair)
                             {
                                 assert(pair.Valid());
                                 return Write(pair);
                             });
    indexInstance->SetReader([&](int64_t offset)
                             {
                                return Read(offset);
                             });
    indexInstance->SetSlowReader([&](std::string_view key)
                                 {
                                     return ReadUnIndexed(key);
                                 });
}

KeyMapped::~KeyMapped()
{
    WriteHeader();
}

bool KeyMapped::Add(std::string_view key, std::string_view value)
{
    if (key.empty() || value.empty())
    {
        KM_WARN("Attempt to write empty key \"{}\" or value \"{}\"", key.data(), value.data());
        return false;
    }

    if (key.size() >= MAX_KEY_SIZE && value.size() >= MAX_VALUE_SIZE)
    {
        KM_WARN("Attempt to write too large key \"{}\" or value \"{}\"", key.data(), value.data());
        return false;
    }

    if (!Get(key).empty())
    {
        return false;
    }

    KeyValue pair{};
    pair.descriptor.keySize = key.size();
    pair.descriptor.valueSize = value.size();
    memcpy(&pair.key, key.data(), key.size());
    memcpy(&pair.value, value.data(), value.size());
    return indexInstance->Add(pair);
}

void KeyMapped::WriteHeader()
{
    std::fstream headerFile(headerPath, std::ios::out | std::ios::binary);
    utils::Write(headerFile, reinterpret_cast<const char*>(&header), sizeof(header));
}

void KeyMapped::ReadHeader()
{
    std::fstream headerFile(headerPath, std::ios::in | std::ios::binary);
    utils::Read(headerFile, reinterpret_cast<char*>(&header), sizeof(header));
}

size_t KeyMapped::Write(const KeyValue& pair)
{
    assert(pair.descriptor.valueSize != 0 && pair.descriptor.keySize != 0);
    assert(pair.descriptor.keySize < MAX_KEY_SIZE && pair.descriptor.valueSize < MAX_VALUE_SIZE);
    header.size += 1;

    return utils::Write(dbFile, reinterpret_cast<const char*>(&pair), sizeof(pair)) - sizeof(pair);
}

KeyValue KeyMapped::Read(int64_t offset)
{
    KeyValue kv{};
    utils::Read(dbFile, reinterpret_cast<char*>(&kv), sizeof(kv), offset);
    return kv;
}

std::string KeyMapped::Get(std::string_view key)
{
    const auto res = indexInstance->Get(key);
    return res.value;
}

std::pair<int64_t, KeyValue> KeyMapped::ReadUnIndexed(std::string_view key)
{
    Timer timer;
    KeyValue pair{};
    bool success = false;
    size_t offset = 0;
    for (int i = 0; i < header.size; i++)
    {
        utils::Read(dbFile, reinterpret_cast<char*>(&pair), sizeof(pair), offset);
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
        return {offset, pair};
    } else
    {
        return {};
    }
}
