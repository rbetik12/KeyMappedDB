#pragma once

#include <memory>
#include <spdlog/logger.h>

namespace db
{
    class Log
    {
    public:
        static void Init();

        inline static std::shared_ptr<spdlog::logger> &GetLogger()
        { return logger; }

    private:
        static std::shared_ptr<spdlog::logger> logger;
        static bool isInitialized;
    };
}

#define KM_TRACE(...) ::db::Log::GetLogger()->trace(__VA_ARGS__)
#define KM_INFO(...)  ::db::Log::GetLogger()->info(__VA_ARGS__)
#define KM_WARN(...)  ::db::Log::GetLogger()->warn(__VA_ARGS__)
#define KM_ERROR(...) ::db::Log::GetLogger()->error(__VA_ARGS__)
#define KM_FATAL(...) ::db::Log::GetLogger()->fatal(__VA_ARGS__)