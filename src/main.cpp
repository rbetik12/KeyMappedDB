#include "KeyMapped.hpp"

int main()
{
    db::KeyMapped db("./db.bin", false);
    db.Add("Key", "Value");
    db.Add("Key1", "Value1");
    db.Add("Key2", "Value2");
    return 0;
}
