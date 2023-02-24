#pragma once

#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <map>

namespace fs = std::filesystem;

namespace db
{
    constexpr const size_t MAGIC_NUMBER = 0x1488;
    constexpr const size_t MAX_KEY_SIZE = 1024;
    constexpr const size_t MAX_VALUE_SIZE = 1024;
    constexpr const size_t MAX_RB_TREE_SIZE = 512;

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
        KeyMapped(const fs::path& dbPath, bool overwrite = false, bool debug = false);
        ~KeyMapped();

        void Add(std::string_view key, std::string_view value);
        std::string Get(std::string_view key);
        size_t Size() const { return header.size; }

    private:
        void WriteHeader();
        void ReadHeader();

        size_t Write(const KeyValue& pair);
        KeyValue Read(std::string_view key);
        KeyValue ReadUnIndexed(std::string_view key);
        KeyValue ReadHashIndex(std::string_view key);

        void DumpRbTree();

        std::shared_ptr<std::ostream> dbFileOutput;
        std::shared_ptr<std::istream> dbFileInput;
        Header header;
        bool showDebugInfo = false;

        std::unordered_map<std::string, size_t> hashIndex;
        std::map<std::string, size_t> rbTreeIndex;
        std::map<std::string, size_t> sparseTable;
    };
}