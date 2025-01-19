#pragma once

#include <osv/types.h>

namespace bench {
    static inline uint64_t rdtsc(void) {
        union {
          uint64_t val;
          struct {
            uint32_t lo;
            uint32_t hi;
          };
        } tsc;
      asm volatile("rdtsc" : "=a"(tsc.lo), "=d"(tsc.hi));
      return tsc.val;
    }

    void evaluate_mmu(void);

    void evaluate_mempool(void);
}
