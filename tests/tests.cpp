#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>

#include <KeyMapped.hpp>
#include <Utils.hpp>

constexpr const char* TEST_DB_PATH = "./test-db.bin";
constexpr const char* TEST_BIN_DIR = "bin";

namespace
{
    void CreateDb(db::index::Type index)
    {
        db::KeyMapped db(TEST_DB_PATH, true, false, index);
    }
}

TEST_CASE("Create and check slow db")
{
    CreateDb(db::index::Type::Slow);
    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::Slow);
    db.Add("Key", "Value");
    db.Add("Key1", "Value1");
    db.Add("Key2", "Value2");
    db.Add("Key3", "Value3");

    CHECK(db.Get("Key") == "Value");
    CHECK(db.Get("Key1") == "Value1");
    CHECK(db.Get("Key2") == "Value2");
    CHECK(db.Get("Key3") == "Value3");
    CHECK(db.Get("") == "");
}

TEST_CASE("Create and check hash db")
{
    CreateDb(db::index::Type::Hash);
    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::Hash);
    db.Add("Key", "Value");
    db.Add("Key1", "Value1");
    db.Add("Key2", "Value2");
    db.Add("Key3", "Value3");

    CHECK(db.Get("Key") == "Value");
    CHECK(db.Get("Key1") == "Value1");
    CHECK(db.Get("Key2") == "Value2");
    CHECK(db.Get("Key3") == "Value3");
    CHECK(db.Get("") == "");
}

TEST_CASE("Check file read and write")
{
    const size_t dataSize = 16;
    const char data[dataSize] = {
            'H', 'L', 'O', 'T', 'E', 'S', 'T'
    };
    const std::string filePath = std::string(TEST_BIN_DIR) + "/test-read-write.bin";

    {
        fs::create_directory(std::string(TEST_BIN_DIR));
        std::ofstream file(filePath);
    }

    std::fstream stream(filePath, std::ios::in | std::ios::out | std::ios::binary);
    CHECK(db::utils::Write(stream, data, dataSize) == dataSize);

    {
        char readData[dataSize] = {};
        CHECK(db::utils::Read(stream, readData, dataSize) == dataSize);
        CHECK(memcmp(readData, data, dataSize) == 0);
    }
    {
        char readData[dataSize] = {};
        const char checkData[dataSize] = {
                'O', 'T', 'E', 'S', 'T'
        };
        CHECK(db::utils::Read(stream, readData, dataSize, 2) == dataSize - 2);
        CHECK(memcmp(readData, checkData, dataSize) == 0);
    }
    {
        char readData[dataSize] = {};
        const char checkData[dataSize] = {
                'O', 'T', 'E', 'S', 'T'
        };
        CHECK(db::utils::Read(stream, readData, dataSize, -14) == 14);
        CHECK(memcmp(readData, checkData, dataSize) == 0);
    }
}
