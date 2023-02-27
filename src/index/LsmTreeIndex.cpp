#include "LsmTreeIndex.hpp"
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
        KeyValue kv = Find(key);
        if (kv.Valid())
        {
            return false;
        }
        table[key] = writer(pair);

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
        for (auto&& [key, path] : sparseTable)
        {
            if (fs::exists(path))
            {
                assert(fs::remove(path));
            }
        }
        sparseTable.clear();
    }

    LSMTreeIndex::~LSMTreeIndex()
    {
        SaveTable();
        SaveSparseTable();
    }

    KeyValue LSMTreeIndex::Find(std::string_view key)
    {
        const auto it = table.find(key.data());
        if (it != table.end())
        {
            return reader(it->second);
        }

        for (const auto& [keyPair, path] : sparseTable)
        {
            if (keyPair.second > key.data() && key.data() > keyPair.first)
            {
                LoadTable(path);
                const auto loadedIt = table.find(key.data());
                if (loadedIt != table.end())
                {
                    return reader(loadedIt->second);
                }
                assert(false);
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

        const std::string& firstKey = table.begin()->first;
        const std::string& lastKey = table.rbegin()->first;
        const std::string indexName = name + "-" + firstKey + "-" + lastKey + "-lsm-tree.index";
        sparseTable[{ firstKey, lastKey }] = indexName;

        std::fstream output(indexName, std::ios::out | std::ios::binary);
        assert(output.good());
        assert(output.is_open());
        utils::Write(output, reinterpret_cast<const char*>(&header), sizeof(header));
        utils::Write(output, reinterpret_cast<const char*>(indexes.data()), indexes.size() * sizeof(OffsetIndex));
        table.clear();
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
        SparseTableHeader header{};
        header.magicNumber = MAGIC_NUMBER;
        header.size = sparseTable.size();
        std::vector<SparseTableElement> elements;
        elements.reserve(sparseTable.size());

        for (const auto& [key, path] : sparseTable)
        {
            assert(key.first.size() < MAX_KEY_SIZE
            && key.second.size() < MAX_KEY_SIZE
            && path.size() < MAX_KEY_SIZE);

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
}
