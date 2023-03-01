#pragma once

#include <IIndex.hpp>
#include <map>

namespace db::index
{
    class SSTableIndex : public IIndex
    {
    public:
        SSTableIndex(const std::string& dbName);
        virtual bool Add(const KeyValue& pair) override;
        virtual KeyValue Get(std::string_view key) override;

        virtual void Clear() override;

        virtual ~SSTableIndex() override;

    private:
        void LoadTable();
        void SaveTable();
        std::map<std::string, std::future<size_t>> table;
    };
}
