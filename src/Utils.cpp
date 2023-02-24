#include "Utils.hpp"

#include <cassert>

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
}