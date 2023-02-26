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
        table[key] = writer(pair);
        return true;
    }

    KeyValue SSTableIndex::Get(std::string_view key)
    {
        KeyValue kv{};
        const std::string keyStr(key);
        const auto it = table.find(keyStr);
        if (it != table.end())
        {
            return reader(it->second);
        }

        auto [offset, result] = slowReader(key);
        if (result.Valid())
        {
            table[keyStr] = offset;
        }
        return result;
    }

    SSTableIndex::~SSTableIndex()
    {
        SaveTable();
    }

    void SSTableIndex::LoadTable()
    {

    }

    void SSTableIndex::SaveTable()
    {
        OffsetIndexHeader header{};
        header.keysAmount = table.size();
        header.magicNumber = MAGIC_NUMBER;
        std::vector<OffsetIndex> indexes;
        indexes.reserve(table.size());

        for (const auto& [key, offset] : table)
        {
            OffsetIndex index{};
            assert(key.size() < MAX_KEY_SIZE);
            memcpy(index.key, key.data(), key.size());
            index.offset = offset;
            indexes.emplace_back(index);
        }

        std::fstream output(name, std::ios::out | std::ios::binary);
        assert(output.good());
        assert(output.is_open());
        utils::Write(output, reinterpret_cast<const char*>(&header), sizeof(header));
        utils::Write(output, reinterpret_cast<const char*>(indexes.data()), indexes.size());
    }
}
