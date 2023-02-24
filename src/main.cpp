#include "KeyMapped.hpp"

int main()
{
    {
        db::KeyMapped db("./db.bin", true);
        db.Add("Key", "Value");
        db.Add("Key1", "Value1");
        db.Add("Key2", "Value2");
    }

    {
        db::KeyMapped db1("./db.bin");
        db1.Add("Key4", "Value4");
    }
    return 0;
}
