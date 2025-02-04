#ifndef TYPEDEFS_HH
#define TYPEDEFS_HH
#include <cstdint>
#include <cstdlib>
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
#ifdef OSV
#include <osv/cache.hh>
#endif

#ifdef MMAP
typedef u64 PID;
uint64_t rdtsc() {
   uint32_t hi, lo;
   __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
   return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
}
#endif

// use when lock is not free
void yield(u64 counter) {
   _mm_pause();
}

u64 envOr(const char* env, u64 value){
   char* newV = std::getenv(env);
   return (newV) ? static_cast<u64>(std::atof(newV)) : value;
}
#ifndef PAGESIZE
#define PAGESIZE 4096
#endif
struct OLCRestartException{};
constexpr u64 pageSize = PAGESIZE;
const u64 mb = 1024ull * 1024;
const u64 gb = 1024ull * 1024 * 1024;
struct Page {
   bool dirty;
};
Page* offset(Page* start, PID pid){
   return reinterpret_cast<Page*>(reinterpret_cast<void*>(start) + pid * pageSize);
}
u64 divise(Page* start, Page* current){
   return (reinterpret_cast<u64>(current) - reinterpret_cast<u64>(start))/pageSize;
}
#endif
