#pragma once

#include <fstream>

namespace db::utils
{
    size_t Write(std::fstream& stream, const char* data, size_t size);
    size_t Read(std::fstream& stream, char* data, size_t size, int64_t offset = 0);
}