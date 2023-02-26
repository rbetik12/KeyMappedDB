#include "SlowIndex.hpp"
#include <KeyMapped.hpp>

namespace db::index
{
    SlowIndex::SlowIndex(const std::string& dbName) : IIndex(dbName)
    {
        name = dbName + "-slow.index";
    }

    bool SlowIndex::Add(const KeyValue& pair)
    {
        writer(pair);
        return true;
    }

    KeyValue SlowIndex::Get(std::string_view key)
    {
        auto result = slowReader(key);
        return result.second;
    }

    SlowIndex::~SlowIndex()
    {}
}
