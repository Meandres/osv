#include <chrono>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#ifdef OSV
#include <osv/cache.hh>
#include <osv/sampler.hh>
#endif
#ifdef LINUX
#include <sys/mman.h>
#endif

using namespace std;

uint64_t envOr(const char* env, uint64_t value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

uint64_t rdtsc() {
    uint32_t hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
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
            fn(i, b, e-1);
        });
   }
   for (auto& t : threads)
      t.join();
}

int main(int argc, char* argv[])
{
    uint64_t nthreads = envOr("THREADS", 64);
    uint64_t explicit_control = envOr("EXPLICIT", 0);
    bool expli = explicit_control == 1 ? true : false;
    constexpr int N = 200000; 
    uint64_t size = nthreads*N*2*4096ULL;
    uint64_t times[nthreads]={0};
    #ifdef LINUX
    char* mem = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if(expli)
        std::cerr << "vmcache,explicit_control," << nthreads << ",";
    else
        std::cerr << "mmap,page_fault," << nthreads << ",";
    #endif
    #ifdef OSV
    CacheManager *cache = createMMIORegion(NULL, size, size, nthreads, 64, expli);
    if(expli)
        std::cerr << "osv,explicit_control," << nthreads << ",";
    else
        std::cerr << "osv,page_fault," << nthreads << ",";
    #endif
    parallel_for(0, nthreads*N, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
        #ifdef OSV
        cache->registerThread();
        #endif
        for (uint64_t i = begin; i < end; i++) {
            auto start = std::chrono::system_clock::now();
            #ifdef OSV
            if(expli)
                cache->fixX(i);
            else
                asm volatile("movq $42, (%0)": : "r" (cache->toPtr(i)): "memory");
            #endif
            #ifdef LINUX
            *(mem+i*4096) = 42;
            #endif
            auto end = std::chrono::system_clock::now();
            times[worker] += chrono::nanoseconds(end-start).count();
        }
        #ifdef OSV
        cache->forgetThread();
        #endif
    });
    uint64_t sum=0;
    for(int i=0; i<nthreads; i++){
        sum+=times[i]/N;
    }
    std::cerr << sum / nthreads << std::endl;
    return 0;
}
