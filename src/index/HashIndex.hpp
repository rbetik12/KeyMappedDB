#pragma once

#include <IIndex.hpp>

namespace db::index
{
    class HashIndex : public IIndex
    {
    public:
        virtual bool Add(const KeyValue& pair) override;
        virtual KeyValue Get(std::string_view key) override;

        virtual ~HashIndex() override;

    private:
        std::unordered_map<std::string, size_t> offsetMap;
    };
}