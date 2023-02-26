#pragma once

#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <map>
#include <IIndex.hpp>
#include <Config.hpp>

namespace fs = std::filesystem;

namespace db
{
    struct Header
    {
        size_t magicNumber = 0;
        size_t size = 0;
    };

    struct KeyValueDescriptor
    {
        size_t keySize = 0;
        size_t valueSize = 0;
    };

    struct KeyValue
    {
        KeyValueDescriptor descriptor;
        char key[MAX_KEY_SIZE];
        char value[MAX_VALUE_SIZE];

        bool Valid() const { return descriptor.keySize > 0 && descriptor.valueSize > 0; }
    };

    class KeyMapped
    {
    public:
        KeyMapped(const fs::path& dbPath, bool overwrite = false, bool debug = false, index::Type indexType = index::Type::Hash);
        ~KeyMapped();

        bool Add(std::string_view key, std::string_view value);
        std::string Get(std::string_view key);
        size_t Size() const { return header.size; }

    private:
        void WriteHeader();
        void ReadHeader();

        size_t Write(const KeyValue& pair);
        KeyValue Read(int64_t offset);
        std::pair<int64_t, KeyValue> ReadUnIndexed(std::string_view key);
        std::fstream dbFile;
        Header header;
        bool showDebugInfo = false;
        std::shared_ptr<index::IIndex> indexInstance;
        fs::path dbPath;
        fs::path headerPath;
    };
}