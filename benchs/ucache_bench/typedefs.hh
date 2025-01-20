#ifndef TYPEDEFS_HH
#define TYPEDEFS_HH
#include <cstdint>
#include <cstdlib>
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

#ifdef MMAP
struct alignas(4096) Page {
   bool dirty;
};
typedef u64 PID;
struct OLCRestartException{};
uint64_t rdtsc() {
   uint32_t hi, lo;
   __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
   return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
}
const u64 pageSize = 4096;
const u64 gb = 1024ull * 1024 * 1024;
#endif

// use when lock is not free
void yield(u64 counter) {
   _mm_pause();
}

u64 envOr(const char* env, u64 value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}
#endif
