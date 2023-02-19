#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <Logger.hpp>

using namespace db;

std::shared_ptr<spdlog::logger> Log::logger;
bool Log::isInitialized = false;

void Log::Init()
{
    if (!isInitialized)
    {
        spdlog::set_pattern("%^[%T] %n: %v%$");
        logger = spdlog::stdout_color_mt("KeyMapped");
        logger->set_level(spdlog::level::trace);
        isInitialized = true;
    }
}

