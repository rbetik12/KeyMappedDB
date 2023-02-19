#pragma once

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace db
{
    constexpr const size_t MAGIC_NUMBER = 0x1488;
    constexpr const size_t MAX_KEY_SIZE = 1024;
    constexpr const size_t MAX_VALUE_SIZE = 1024;

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

        void Write(const KeyValue& pair);
        KeyValue Read(std::string_view key);
        KeyValue ReadUnIndexed(std::string_view key);

        std::shared_ptr<std::ostream> dbFileOutput;
        std::shared_ptr<std::istream> dbFileInput;
        Header header;
        bool showDebugInfo = false;
    };
}