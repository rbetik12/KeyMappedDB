#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>

#include <KeyMapped.hpp>
#include <Utils.hpp>

constexpr const char* TEST_DB_PATH = "./test-db.bin";
constexpr const char* TEST_BIN_DIR = "bin";

namespace
{
    struct KeyValue
    {
        std::string key;
        std::string value;
    };
}

//TEST_CASE("Create and check slow db")
//{
//    CreateDb(db::index::Type::Slow);
//    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::Slow);
//    db.Add("Key", "Value");
//    db.Add("Key1", "Value1");
//    db.Add("Key2", "Value2");
//    db.Add("Key3", "Value3");
//
//    CHECK(db.Get("Key") == "Value");
//    CHECK(db.Get("Key1") == "Value1");
//    CHECK(db.Get("Key2") == "Value2");
//    CHECK(db.Get("Key3") == "Value3");
//    CHECK(db.Get("") == "");
//}

//TEST_CASE("Create and check hash db")
//{
//    CreateDb(db::index::Type::Hash);
//    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::Hash);
//    db.Add("Key", "Value");
//    db.Add("Key1", "Value1");
//    db.Add("Key2", "Value2");
//    db.Add("Key3", "Value3");
//
//    CHECK(db.Get("Key") == "Value");
//    CHECK(db.Get("Key1") == "Value1");
//    CHECK(db.Get("Key2") == "Value2");
//    CHECK(db.Get("Key3") == "Value3");
//    CHECK(db.Get("") == "");
//}

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

//TEST_CASE("Check empty key in hash db and slow reading")
//{
//    {
//        db::KeyMapped db(TEST_DB_PATH, true, false, db::index::Type::Hash);
//        db.Add("Key", "Value");
//        db.Add("Key1", "Value1");
//        db.Add("Key2", "Value2");
//        db.Add("Key3", "Value3");
//    }
//
//    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::Hash);
//
//    CHECK(db.Get("Key") == "Value");
//    CHECK(db.Get("Key1") == "Value1");
//    CHECK(db.Get("Key2") == "Value2");
//    CHECK(db.Get("Key3") == "Value3");
//    CHECK(db.Get("") == "");
//}

//TEST_CASE("Check empty key in sstable db and slow reading")
//{
//    {
//        db::KeyMapped db(TEST_DB_PATH, true, false, db::index::Type::SSTable);
//        db.Add("Key", "Value");
//        db.Add("Key1", "Value1");
//        db.Add("Key2", "Value2");
//        db.Add("Key3", "Value3");
//    }
//
//    db::KeyMapped db(TEST_DB_PATH, false, false, db::index::Type::SSTable);
//
//    CHECK(db.Get("Key") == "Value");
//    CHECK(db.Get("Key1") == "Value1");
//    CHECK(db.Get("Key2") == "Value2");
//    CHECK(db.Get("Key3") == "Value3");
//    CHECK(db.Get("Kek") == "");
//}

TEST_CASE("Check lsm-tree base")
{
    {
        db::KeyMapped db(TEST_DB_PATH, true, db::index::Type::LSM);
        db.Add("Key", "Value");
        db.Add("Key1", "Value1");
        db.Add("Key2", "Value2");
        db.Add("Key3", "Value3");
    }

    db::KeyMapped db(TEST_DB_PATH, false,  db::index::Type::LSM);

    CHECK(db.Get("Key") == "Value");
    CHECK(db.Get("Key1") == "Value1");
    CHECK(db.Get("Key2") == "Value2");
    CHECK(db.Get("Key3") == "Value3");
    CHECK(db.Get("Kek") == "");
}

#include "../src/index/LsmTreeIndex.hpp"

namespace
{
    void LoadAndCheckLSMTreeDB()
    {
        db::KeyMapped db(TEST_DB_PATH, false, db::index::Type::LSM);
        auto index = std::dynamic_pointer_cast<db::index::LSMTreeIndex>(db.Index());
        assert(index != nullptr);
        index->SetMaxTableSize(2);

        CHECK(db.Get("Key") == "Value");
        CHECK(db.Get("Key1") == "Value1");
        CHECK(db.Get("Key2") == "Value2");
        CHECK(db.Get("Key3") == "Value3");
        CHECK(db.Get("Kek") == "");
    }
}

TEST_CASE("Check lsm-tree base with low table size dump value")
{
    {
        db::KeyMapped db(TEST_DB_PATH, true, db::index::Type::LSM);
        auto index = std::dynamic_pointer_cast<db::index::LSMTreeIndex>(db.Index());
        assert(index != nullptr);
        index->SetMaxTableSize(2);
        db.Add("Key", "Value");
        db.Add("Key1", "Value1");
        db.Add("Key2", "Value2");
        db.Add("Key3", "Value3");
    }

    LoadAndCheckLSMTreeDB();
    LoadAndCheckLSMTreeDB();
}

TEST_CASE("Check lsm-tree with 1000 key-value pairs")
{
    db::KeyMapped db(TEST_DB_PATH, true, db::index::Type::LSM);

    const size_t samplesAmount = 1000;
    std::vector<KeyValue> kvs;
    kvs.reserve(samplesAmount);

    for (size_t i = 0; i < samplesAmount; i++)
    {
        KeyValue kv{};
        kv.key = db::utils::GenerateRandomString(16);
        kv.value = db::utils::GenerateRandomString(16);
        kvs.emplace_back(kv);
    }

    for (const auto& [key, value]: kvs)
    {
        db.Add(key, value);
    }

    bool result = true;
    for (const auto& [key, value] : kvs)
    {
        result &= db.Get(key) == value;
    }
    CHECK(result == true);
}

TEST_CASE("Check lsm-tree with low table size dump value and large keys")
{
    std::vector<KeyValue> kvs;
    kvs.reserve(20);

    for (size_t i = 0; i < 20; i++)
    {
        KeyValue kv{};
        kv.key = db::utils::GenerateRandomString(db::MAX_KEY_SIZE - 1);
        kv.value = db::utils::GenerateRandomString(db::MAX_KEY_SIZE - 1);
        kvs.emplace_back(kv);
    }

    {
        db::KeyMapped db(TEST_DB_PATH, true, db::index::Type::LSM);
        auto index = std::dynamic_pointer_cast<db::index::LSMTreeIndex>(db.Index());
        assert(index != nullptr);
        index->SetMaxTableSize(2);
        for (const auto& [key, value] : kvs)
        {
            db.Add(key, value);
        }
    }

    db::KeyMapped db(TEST_DB_PATH, false, db::index::Type::LSM);
    auto index = std::dynamic_pointer_cast<db::index::LSMTreeIndex>(db.Index());
    assert(index != nullptr);
    index->SetMaxTableSize(2);

    bool result = true;
    for (const auto& [key, value] : kvs)
    {
        result &= db.Get(key) == value;
    }
    CHECK(result == true);
}
