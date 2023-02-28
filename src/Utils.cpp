#include "Utils.hpp"

#include <cassert>
#include <random>
#include <sstream>

namespace db::utils
{
    size_t Write(std::fstream& stream, const char* data, size_t size)
    {
        assert(data);
        assert(size > 0);

        stream.write(data, size);
        stream.flush();
        return static_cast<size_t>(stream.tellp());
    }

    size_t Read(std::fstream& stream, char* data, size_t size, int64_t offset)
    {
        assert(data);
        assert(size > 0);

        const std::streampos pos = stream.tellg();
        if (offset < 0)
        {
            stream.seekg(offset, std::ios::end);
        }
        else
        {
            stream.seekg(offset, std::ios::beg);
        }

        stream.read(data, size);
        const size_t bytesRead = static_cast<size_t>(stream.gcount());
        if (stream.eof())
        {
            stream.clear();
            stream.seekg(0, std::ios::beg);
        }
        else
        {
            stream.seekg(pos);
        }

        return bytesRead;
    }

    std::string GenerateRandomString(int length)
    {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static constexpr char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static constexpr int alphabetSize = sizeof(alphabet) - 1;
        static char distribution[alphabetSize];

        // Initialize the distribution array with random characters
        std::generate_n(distribution, alphabetSize, [&] { return alphabet[gen() % alphabetSize]; });

        std::ostringstream randomString;
        for (int i = 0; i < length; ++i)
        {
            randomString << distribution[i % alphabetSize];
        }

        return randomString.str();
    }
}