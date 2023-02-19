#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>

#include <KeyMapped.hpp>

constexpr const char* TEST_DB_PATH = "./test-db.bin";

TEST_CASE("Test db size")
{
    {
        db::KeyMapped db(TEST_DB_PATH, true);
        db.Add("Key", "Value");
        db.Add("Key1", "Value1");
        db.Add("Key2", "Value2");
    }

    db::KeyMapped db(TEST_DB_PATH);
    CHECK(db.Size() == 3);
}