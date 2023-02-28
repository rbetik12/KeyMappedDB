#include <KeyMapped.hpp>
#include <Config.hpp>
#include <Timer.hpp>
#include <Utils.hpp>

namespace
{
    struct KeyValue
    {
        std::string key;
        std::string value;
    };
    constexpr const char* PERF_TEST_DB_PATH = "./perf-test-db.bin";
    constexpr const size_t SAMPLES_AMOUNT = 1000;
}

int main()
{
    std::vector<KeyValue> kvs;
    kvs.reserve(SAMPLES_AMOUNT);

    for (size_t i = 0; i < SAMPLES_AMOUNT; i++)
    {
        KeyValue kv{};
        kv.key = db::utils::GenerateRandomString(db::MAX_KEY_SIZE - 1);
        kv.value = db::utils::GenerateRandomString(db::MAX_KEY_SIZE - 1);
        kvs.emplace_back(kv);
    }

    db::KeyMapped db(PERF_TEST_DB_PATH, true, false, db::index::Type::LSM);

    {
        db::Timer timer("Filling table");
        for (const auto& [key, value]: kvs)
        {
            db.Add(key, value);
        }
    }

    {
        db::Timer timer("Reading table");
        for (const auto& [key, value]: kvs)
        {
            db.Get(key);
        }
    }

    return 0;
}
