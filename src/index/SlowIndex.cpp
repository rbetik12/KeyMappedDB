#include "SlowIndex.hpp"
#include <KeyMapped.hpp>

namespace db::index
{
    bool SlowIndex::Add(const KeyValue& pair)
    {
        writer(pair);
        return true;
    }

    KeyValue SlowIndex::Get(std::string_view key)
    {
        auto result = slowReader(key);
        return result;
    }

    SlowIndex::~SlowIndex()
    {}
}
