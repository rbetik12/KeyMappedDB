#include "HashIndex.hpp"
#include <KeyMapped.hpp>

namespace db::index
{
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
        if (it == offsetMap.end())
        {
            return kv;
        }

        return reader(it->second);
    }

    HashIndex::~HashIndex()
    {}
}