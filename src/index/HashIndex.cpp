#include "HashIndex.hpp"
#include <KeyMapped.hpp>

namespace db::index
{
    HashIndex::HashIndex(const std::string& dbName) : IIndex(dbName)
    {
        name = dbName + "-hash.index";
    }

    bool HashIndex::Add(const KeyValue& pair)
    {
        const std::string key = pair.key;
        if (offsetMap.find(key) != offsetMap.end())
        {
            return false;
        }
        offsetMap[key] = writer(pair);
        return true;
    }

    KeyValue HashIndex::Get(std::string_view key)
    {
        KeyValue kv{};
        const std::string keyStr(key);
        const auto it = offsetMap.find(keyStr);
        if (it != offsetMap.end())
        {
            reader(it->second);
        }

        auto [offset, result] = slowReader(key);
        if (result.Valid())
        {
            offsetMap[keyStr] = offset;
        }
        return result;
    }

    HashIndex::~HashIndex()
    {}
}