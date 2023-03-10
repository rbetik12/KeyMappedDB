#pragma once

#include <filesystem>

namespace fs = std::filesystem;

namespace db
{
    constexpr const size_t BLOCK_SIZE = 512;
    constexpr const size_t MAGIC_NUMBER = 0x1488;
    constexpr const size_t MAX_KEY_SIZE = BLOCK_SIZE;
    constexpr const size_t MAX_VALUE_SIZE = BLOCK_SIZE;
    const size_t MAX_SSTABLE_SIZE = static_cast<size_t>(std::powl(2, 15));
}