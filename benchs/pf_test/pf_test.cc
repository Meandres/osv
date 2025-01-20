#include <chrono>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <osv/cache.hh>
#include <sys/mman.h>

using namespace std;

u64 envOr(const char* env, u64 value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

int stick_this_thread_to_core(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

template<class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
    std::vector<std::thread> threads;
    uint64_t n = end-begin;
    uint64_t perThread = n/nthreads;
    for (unsigned i=0; i<nthreads; i++) {
        threads.emplace_back([&,i]() {
            stick_this_thread_to_core(i);
            uint64_t b = (perThread*i) + begin;
            uint64_t e = (i==(nthreads-1)) ? end : ((b+perThread) + begin);
            fn(i, b, e);
        });
   }
   for (auto& t : threads)
      t.join();
}

int main(int argc, char* argv[])
{
    uint64_t nthreads = envOr("THREADS", 1);
    uint64_t nbPages = envOr("PAGES", 100000);
    u64 size = nbPages*nthreads*pageSize;
    CacheManager* cm = createMMIORegion(NULL, size, size, 64);
    cm->record_time = true;
    parallel_for(0, nthreads*nbPages, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
        for (uint64_t i = begin; i < end; i++) {
            cm->begin_pf_time = rdtsc();
            memset(cm->virtMem+i, 0, pageSize);
        }
    });
    std::cerr << "pf_time: "<< cm->acc_pf_time / cm->cnt << std::endl;
    return 0;
}
