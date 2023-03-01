#include "SsTableIndex.hpp"
#include <KeyMapped.hpp>
#include <Utils.hpp>
#include <cassert>

namespace db::index
{
    SSTableIndex::SSTableIndex(const std::string& dbName) : IIndex(dbName)
    {
        name = dbName + "-sstable.index";
        LoadTable();
    }

    bool SSTableIndex::Add(const KeyValue& pair)
    {
        const std::string key = pair.key;
        if (table.find(key) != table.end())
        {
            return false;
        }
        table[key] = std::move(writer(pair));
        return true;
    }

    KeyValue SSTableIndex::Get(std::string_view key)
    {
        KeyValue kv{};
        const std::string keyStr(key);
        const auto it = table.find(keyStr);
        if (it != table.end())
        {
            return reader(it->second.get());
        }

        auto [offset, result] = slowReader(key);
        if (result.Valid())
        {
            std::promise<size_t> promise;
            auto future = promise.get_future();
            promise.set_value(offset);
            table[keyStr] = std::move(future);
        }
        return result;
    }

    SSTableIndex::~SSTableIndex()
    {
        SaveTable();
    }

    void SSTableIndex::LoadTable()
    {
        if (!fs::exists(name))
        {
            return;
        }

        OffsetIndexHeader header{};
        std::vector<OffsetIndex> indexes;

        std::fstream input(name, std::ios::in | std::ios::binary);
        assert(input.good());
        assert(input.is_open());

        utils::Read(input, reinterpret_cast<char*>(&header), sizeof(header));
        assert(header.magicNumber == MAGIC_NUMBER);
        indexes.resize(header.keysAmount);
        utils::Read(input, reinterpret_cast<char*>(indexes.data()), header.keysAmount * sizeof(OffsetIndex), sizeof(header));

        for (const auto& index : indexes)
        {
            std::promise<size_t> promise;
            promise.set_value(index.offset);
            table[index.key] = std::move(promise.get_future());
        }
    }

    void SSTableIndex::SaveTable()
    {
        OffsetIndexHeader header{};
        header.keysAmount = table.size();
        header.magicNumber = MAGIC_NUMBER;
        std::vector<OffsetIndex> indexes;
        indexes.reserve(table.size());

        for (auto& [key, offset] : table)
        {
            OffsetIndex index{};
            assert(key.size() < MAX_KEY_SIZE);
            memcpy(index.key, key.data(), key.size());
            index.offset = offset.get();
            indexes.emplace_back(index);
        }

        std::fstream output(name, std::ios::out | std::ios::binary);
        assert(output.good());
        assert(output.is_open());
        utils::Write(output, reinterpret_cast<const char*>(&header), sizeof(header));
        utils::Write(output, reinterpret_cast<const char*>(indexes.data()), indexes.size() * sizeof(OffsetIndex));
    }

    void SSTableIndex::Clear()
    {
        IIndex::Clear();
        table.clear();
    }
}
