#include "LsmTreeIndex.hpp"
#include "Timer.hpp"
#include <Utils.hpp>
#include <KeyMapped.hpp>

namespace db::index
{

    LSMTreeIndex::LSMTreeIndex(const std::string& dbName) : IIndex(dbName)
    {
        name = dbName;
        LoadSparseTable();
    }

    bool LSMTreeIndex::Add(const KeyValue& pair)
    {
        const std::string key = pair.key;
        if (Exists(key))
        {
            return false;
        }

        pendingTable[key] = writer(pair);
        if (table.size() > maxTableSize)
        {
            SaveTable();
        }
        return true;
    }

    KeyValue LSMTreeIndex::Get(std::string_view key)
    {
        return Find(key);
    }

    void LSMTreeIndex::Clear()
    {
        IIndex::Clear();
        table.clear();
        ClearSparseTable();
    }

    LSMTreeIndex::~LSMTreeIndex()
    {
        FlushPending();
        SaveTable();

        //Merge indexes
        for (const auto& [key, path] : sparseTable)
        {
            LoadTable(path);
        }
        ClearSparseTable();
        SaveTable();
        SaveSparseTable();
    }

    KeyValue LSMTreeIndex::Find(std::string_view key)
    {
        const auto value = GetCachedValue(key);
        if (value.has_value())
        {
            return reader(value.value());
        }

        for (const auto& [keyPair, path] : sparseTable)
        {
            const std::string keySbtr(key.substr(0, indexKeySize));
            if (keyPair.second > keySbtr && keySbtr > keyPair.first)
            {
                LoadTable(path);
                const auto loadedIt = table.find(key.data());
                if (loadedIt != table.end())
                {
                    return reader(loadedIt->second);
                }
            }
        }

        const auto [offset, kv] = slowReader(key);
        if (kv.Valid())
        {
            table[key.data()] = offset;
        }
        return kv;
    }

    void LSMTreeIndex::SaveTable()
    {
        FlushPending();
        if (table.empty())
        {
            return;
        }

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
            index.offset = offset;
            indexes.emplace_back(index);
        }

        const std::string firstKey = table.begin()->first.substr(0, indexKeySize);
        const std::string lastKey = (--table.end())->first.substr(0, indexKeySize);
        const std::string indexName = name + "-" + firstKey + "-" + lastKey + "-lsm-tree.index";
        sparseTable[{ firstKey, lastKey }] = indexName;

        std::fstream output(indexName, std::ios::out | std::ios::binary);
        assert(output.good());
        assert(output.is_open());
        utils::Write(output, reinterpret_cast<const char*>(&header), sizeof(header));
        utils::Write(output, reinterpret_cast<const char*>(indexes.data()), indexes.size() * sizeof(OffsetIndex));
    }

    void LSMTreeIndex::LoadTable(std::string_view tableName)
    {
        if (!fs::exists(tableName))
        {
            return;
        }

        OffsetIndexHeader header{};
        std::vector<OffsetIndex> indexes;

        std::fstream input(tableName.data(), std::ios::in | std::ios::binary);
        assert(input.good());
        assert(input.is_open());

        utils::Read(input, reinterpret_cast<char*>(&header), sizeof(header));
        assert(header.magicNumber == MAGIC_NUMBER);
        indexes.resize(header.keysAmount);
        utils::Read(input, reinterpret_cast<char*>(indexes.data()), header.keysAmount * sizeof(OffsetIndex), sizeof(header));

        for (const auto& index : indexes)
        {
            table[index.key] = index.offset;
        }
    }

    void LSMTreeIndex::SaveSparseTable()
    {
        if (sparseTable.empty())
        {
            return;
        }

        SparseTableHeader header{};
        header.magicNumber = MAGIC_NUMBER;
        header.size = sparseTable.size();
        std::vector<SparseTableElement> elements;
        elements.reserve(sparseTable.size());

        for (const auto& [key, path] : sparseTable)
        {
            assert(key.first.size() < MAX_KEY_SIZE
            && key.second.size() < MAX_KEY_SIZE
            && path.size() < MAX_PATH_LENGTH);

            SparseTableElement element{};
            memcpy(element.firstKey, key.first.data(), key.first.size());
            memcpy(element.lastKey, key.second.data(), key.second.size());
            memcpy(element.indexPath, path.data(), path.size());

            elements.emplace_back(element);
        }

        const std::string path = name + "-sparse-table.index";
        std::fstream output(path, std::ios::out | std::ios::binary);
        assert(output.good());
        assert(output.is_open());
        utils::Write(output, reinterpret_cast<const char*>(&header), sizeof(header));
        utils::Write(output, reinterpret_cast<const char*>(elements.data()), elements.size() * sizeof(elements.at(0)));
    }

    void LSMTreeIndex::LoadSparseTable()
    {
        const std::string path = name + "-sparse-table.index";
        if (!fs::exists(path))
        {
            return;
        }

        SparseTableHeader header{};
        std::vector<SparseTableElement> elements;
        std::fstream input(path, std::ios::in | std::ios::binary);
        assert(input.good());
        assert(input.is_open());

        utils::Read(input, reinterpret_cast<char*>(&header), sizeof(header));
        assert(header.magicNumber == MAGIC_NUMBER);
        elements.resize(header.size);
        utils::Read(input, reinterpret_cast<char*>(elements.data()), header.size * sizeof(elements.at(0)), sizeof(header));
        for (const auto& element : elements)
        {
            sparseTable[{ element.firstKey, element.lastKey }] = element.indexPath;
        }
    }

    void LSMTreeIndex::ClearSparseTable()
    {
        for (auto&& [key, path] : sparseTable)
        {
            if (fs::exists(path))
            {
                assert(fs::remove(path));
            }
        }
        sparseTable.clear();
    }

    std::optional<size_t> LSMTreeIndex::GetCachedValue(std::string_view key)
    {
        auto pendingIt = pendingTable.find(key.data());
        if (pendingIt != pendingTable.end())
        {
            auto& future = pendingIt->second;
            assert(future.valid());
            auto value = future.get();
            table[key.data()] = value;
            pendingTable.erase(key.data());
            return value;
        }
        auto it = table.find(key.data());
        if (it == table.end())
        {
            return std::nullopt;
        }
        return it->second;
    }

    bool LSMTreeIndex::Exists(std::string_view key)
    {
        auto pendingIt = pendingTable.find(key.data());
        if (pendingIt != pendingTable.end())
        {
            return true;
        }

        auto it = table.find(key.data());
        if (it != table.end())
        {
            return true;
        }

        for (const auto& [keyPair, path] : sparseTable)
        {
            const std::string keySbtr(key.substr(0, indexKeySize));
            if (keyPair.second > keySbtr && keySbtr > keyPair.first)
            {
                LoadTable(path);
                const auto loadedIt = table.find(key.data());
                if (loadedIt != table.end())
                {
                    return true;
                }
            }
        }
        return false;
    }

    void LSMTreeIndex::FlushPending()
    {
        if (!pendingTable.empty())
        {
            for (auto it = pendingTable.begin(); it != pendingTable.end();)
            {
                assert(it->second.valid());
                table[it->first] = it->second.get();
                it = pendingTable.erase(it);
            }
        }
    }

    void LSMTreeIndex::Flush()
    {
        IIndex::Flush();
        FlushPending();
    }
}
