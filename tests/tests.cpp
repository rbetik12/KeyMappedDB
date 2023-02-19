#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>

#include <KeyMapped.hpp>

constexpr const char* TEST_DB_PATH = "./test-db.bin";

namespace
{
    void CreateDb()
    {
        db::KeyMapped db(TEST_DB_PATH, true);
        db.Add("Key", "Value");
        db.Add("Key1", "Value1");
        db.Add("Key2", "Value2");
    }
}

TEST_CASE("Database size")
{
    CreateDb();

    db::KeyMapped db(TEST_DB_PATH);
    CHECK(db.Size() == 3);
}

TEST_CASE("Read and write")
{
    CreateDb();

    db::KeyMapped db(TEST_DB_PATH);
    CHECK(db.Get("Key") == "Value");
    CHECK(db.Get("Key1") == "Value1");
    CHECK(db.Get("Key2") == "Value2");
    CHECK(db.Get("Key3") == "");
}
