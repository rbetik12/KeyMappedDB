#pragma once

#include <chrono>
#include <utility>

#define TIMER_START { Timer timer;
#define TIMER_END const auto elapsed = timer.ElapsedMillis(); \
KM_INFO("{} executed for {}ms", __FUNCTION__, elapsed);\
}

namespace db {
    class Timer
    {
    public:
        Timer(std::string debugStr) : str(std::move(debugStr))
        {
            Reset();
        }

        ~Timer()
        {
            KM_INFO("{}: {}ms", str, ElapsedMillis());
        }

        void Reset()
        {
            start = std::chrono::high_resolution_clock::now();
        }

        float Elapsed()
        {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start).count() * 0.001f * 0.001f * 0.001f;
        }

        float ElapsedMillis()
        {
            return Elapsed() * 1000.0f;
        }

    private:
        std::chrono::time_point<std::chrono::high_resolution_clock> start;
        std::string str;
    };
}