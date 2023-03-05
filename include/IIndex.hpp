#pragma once

#include <functional>
#include <string>
#include <Config.hpp>
#include <future>
#include "Logger.hpp"

namespace db
{
    struct KeyValue;
}

namespace db::index
{
    struct OffsetIndexHeader
    {
        size_t magicNumber;
        size_t keysAmount;
    };

    struct OffsetIndex
    {
        char key[MAX_KEY_SIZE];
        size_t offset;
    };

    enum class Type
    {
        Slow,
        Hash,
        SSTable,
        LSM
    };

    class IIndex
    {
    public:
        IIndex(const std::string& dbName)
        {
            if (name.empty())
            {
                name = dbName;
            }
        }
        virtual bool Add(const KeyValue& pair) = 0;
        virtual KeyValue Get(std::string_view key) = 0;

        virtual void Clear()
        {
            if (fs::exists(name))
            {
                assert(fs::remove(name));
            }
        }

        virtual void Flush() {}

        virtual ~IIndex() {}

        void SetWriter(std::function<std::future<size_t>(const KeyValue&)>&& aWriter)
        { writer = std::move(aWriter); }

        void SetReader(std::function<KeyValue(int64_t)>&& aReader)
        { reader = std::move(aReader); }

        void SetSlowReader(std::function<std::pair<int64_t, KeyValue>(std::string_view)>&& aReader)
        { slowReader = aReader; }

    protected:
        std::function<std::future<size_t>(const KeyValue&)> writer;
        std::function<KeyValue(int64_t)> reader;
        std::function<std::pair<int64_t, KeyValue>(std::string_view)> slowReader;
        std::string name;
    };
}