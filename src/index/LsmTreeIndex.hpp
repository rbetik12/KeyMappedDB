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

        void SetMaxTableSize(size_t size)
        { maxTableSize = size; }

    private:
        static const size_t MAX_PATH_LENGTH = 256;

        struct SparseTableHeader
        {
            size_t magicNumber;
            size_t size;
        };

        struct SparseTableElement
        {
            char firstKey[MAX_KEY_SIZE];
            char lastKey[MAX_KEY_SIZE];
            char indexPath[MAX_PATH_LENGTH];
        };

        KeyValue Find(std::string_view key);
        bool Exists(std::string_view key);
        void SaveTable();
        void LoadTable(std::string_view tableName);
        void SaveSparseTable();
        void LoadSparseTable();
        void ClearSparseTable();
        void FlushPending();

        std::optional<size_t> GetCachedValue(std::string_view key);

        std::unordered_map<std::string, std::future<size_t>> pendingTable;
        std::map<std::string, size_t> table;
        std::map<std::pair<std::string, std::string>, std::string> sparseTable;
        size_t maxTableSize = MAX_SSTABLE_SIZE;
        const size_t indexKeySize = 19;
    };
}
