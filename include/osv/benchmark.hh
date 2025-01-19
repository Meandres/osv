#pragma once

#include <osv/types.h>
#include <chrono>

namespace bench {
static inline uint64_t rdtsc(void)
{
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

void evaluate_mmu(void);

void evaluate_mempool(void);
}

#define MEASURE(sum, op)                       \
    do {                                       \
        uint64_t bench_start = bench::rdtsc(); \
        {                                      \
            op;                                \
        }                                      \
        uint64_t bench_end = bench::rdtsc();   \
        sum += bench_end - bench_start;        \
    } while (0)
