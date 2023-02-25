#pragma once

#include <functional>
#include <string>

namespace db
{
    struct KeyValue;
}

namespace db::index
{
    enum class Type
    {
        Slow,
        Hash,
        LSM
    };

    class IIndex
    {
    public:
        virtual bool Add(const KeyValue& pair) = 0;
        virtual KeyValue Get(std::string_view key) = 0;

        virtual ~IIndex() {}

        void SetWriter(std::function<size_t(const KeyValue&)>&& aWriter)
        { writer = std::move(aWriter); }

        void SetReader(std::function<KeyValue(int64_t)>&& aReader)
        { reader = std::move(aReader); }

        void SetSlowReader(std::function<KeyValue(std::string_view)>&& aReader)
        { slowReader = aReader; }

    protected:
        std::function<size_t(const KeyValue&)> writer;
        std::function<KeyValue(int64_t)> reader;
        std::function<KeyValue(std::string_view)> slowReader;
        std::string name;
    };
}