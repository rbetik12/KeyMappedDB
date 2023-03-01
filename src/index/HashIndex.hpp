#pragma once

#include <IIndex.hpp>

namespace db::index
{
    class HashIndex : public IIndex
    {
    public:
        HashIndex(const std::string& dbName);

        virtual bool Add(const KeyValue& pair) override;
        virtual KeyValue Get(std::string_view key) override;

        virtual void Clear() override;

        virtual ~HashIndex() override;

    private:
        std::unordered_map<std::string, std::future<size_t>> offsetMap;
    };
}