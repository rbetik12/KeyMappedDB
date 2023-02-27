#pragma once

#include "IIndex.hpp"
#include <map>

namespace db::index
{
    class LSMTreeIndex : public IIndex
    {
    public:
        LSMTreeIndex(const std::string& dbName);
        virtual bool Add(const KeyValue& pair) override;
        virtual KeyValue Get(std::string_view key) override;

        virtual void Clear() override;

        virtual ~LSMTreeIndex() override;

    private:
        struct SparseTableHeader
        {
            size_t magicNumber;
            size_t size;
        };

        struct SparseTableElement
        {
            char firstKey[MAX_KEY_SIZE];
            char lastKey[MAX_KEY_SIZE];
            char indexPath[MAX_KEY_SIZE];
        };

        KeyValue Find(std::string_view key);
        void SaveTable();
        void LoadTable(std::string_view tableName);
        void SaveSparseTable();
        void LoadSparseTable();

        std::map<std::string, size_t> table;
        std::map<std::pair<std::string, std::string>, std::string> sparseTable;
    };
}
