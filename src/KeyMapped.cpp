#include "KeyMapped.hpp"
#include <Logger.hpp>
#include <cassert>
#include <Timer.hpp>
#include <Utils.hpp>
#include "index/HashIndex.hpp"
#include "index/SlowIndex.hpp"
#include "index/SsTableIndex.hpp"
#include "index/LsmTreeIndex.hpp"

using namespace db;

KeyMapped::KeyMapped(const fs::path& dbPath, bool overwrite, index::Type indexType) : dbPath(std::move(dbPath))
{
    Log::Init();
    const std::string headerFilePath = dbPath.stem().generic_string();
    const std::string dbFilePathWExt = headerFilePath + ".meta";
    headerPath = dbFilePathWExt;
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
        case index::Type::LSM:
            indexInstance = std::make_shared<index::LSMTreeIndex>(headerFilePath);
            break;
        default:
            assert(false);
    }

    indexInstance->SetWriter([&](const KeyValue& pair)
                             {
                                 assert(pair.Valid());
                                 return pool.enqueue([=]()
                                 {
                                     return Write(pair);
                                 });
                             });
    indexInstance->SetReader([&](int64_t offset)
                             {
                                 return Read(offset);
                             });
    indexInstance->SetSlowReader([&](std::string_view key)
                                 {
                                     return ReadUnIndexed(key);
                                 });
    if (overwrite && fs::exists(dbPath) && fs::exists(dbPath))
    {
        assert(fs::remove(dbPath));
        assert(fs::remove(headerPath));
        indexInstance->Clear();
    }

    bool validate = false;
    if (fs::exists(dbPath))
    {
        validate = true;
    }

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
}

KeyMapped::~KeyMapped()
{
    indexInstance->Flush();
    WriteHeader();
}

bool KeyMapped::Add(std::string_view key, std::string_view value)
{
    if (key.empty() || value.empty())
    {
        return false;
    }

    if (key.size() >= MAX_KEY_SIZE && value.size() >= MAX_VALUE_SIZE)
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
    std::lock_guard<std::mutex> lock(mutex);
    assert(pair.descriptor.valueSize != 0 && pair.descriptor.keySize != 0);
    assert(pair.descriptor.keySize < MAX_KEY_SIZE && pair.descriptor.valueSize < MAX_VALUE_SIZE);
    header.size += 1;

    const auto offset = utils::Write(dbFile, reinterpret_cast<const char*>(&pair), sizeof(pair)) - sizeof(pair);
    return offset;
}

KeyValue KeyMapped::Read(int64_t offset)
{
    std::lock_guard<std::mutex> lock(mutex);
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
    KeyValue pair{};
    bool success = false;
    size_t offset = 0;
    for (int i = 0; i < header.size; i++)
    {
        pair = Read(offset);
        offset += sizeof(pair);

        if (std::string(key) == pair.key)
        {
            success = true;
            break;
        }
    }

    if (success)
    {
        return {offset, pair};
    } else
    {
        return {};
    }
}
