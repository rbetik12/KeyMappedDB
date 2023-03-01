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
        offsetMap[key] = std::move(writer(pair));
        return true;
    }

    KeyValue HashIndex::Get(std::string_view key)
    {
        KeyValue kv{};
        const std::string keyStr(key);
        const auto it = offsetMap.find(keyStr);
        if (it != offsetMap.end())
        {
            reader(it->second.get());
        }

        auto [offset, result] = slowReader(key);
        if (result.Valid())
        {
            std::promise<size_t> promise;
            promise.set_value(offset);
            offsetMap[keyStr] = std::move(promise.get_future());
        }
        return result;
    }

    HashIndex::~HashIndex()
    {}

    void HashIndex::Clear()
    {
        IIndex::Clear();
        offsetMap.clear();
    }
}