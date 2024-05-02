#include <chrono>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <osv/cache.hh>
#include <osv/sampler.hh>

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
            fn(i, b, e-1);
        });
   }
   for (auto& t : threads)
      t.join();
}

int main()
{
    u64 nthreads = envOr("THREADS", 64);
    u64 explicit_control = envOr("EXPLICIT", 1);
    u64 kernel_func = envOr("KERNEL", 1);
    bool expli = explicit_control == 1 ? true : false;
    bool in_kernel = kernel_func == 1 ? true : false;
    constexpr int N = 200000;
    CacheManager *cache = createMMIORegion(NULL, nthreads*N*2*4096ULL, nthreads*N*2*4096ULL, nthreads, 64, expli);

    if(expli)
        std::cout << "osv,explicit_control," << nthreads << ",";
    else
        std::cout << "osv,page_fault," << nthreads << ",";
    if(in_kernel){
        std::cout << "kernel,";
    }else{
        std::cout << "app,";
    }
    auto start = std::chrono::system_clock::now();
    if(in_kernel){
        cache->execute_swipe_in_kernel(nthreads, nthreads*N, expli);
    }else{
        parallel_for(0, nthreads*N, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
            cache->registerThread();
            for (uint64_t i = begin; i < end; i++) {
                if(expli)
                    cache->fixX(i);
                else
                    asm volatile("movq $42, (%0)": : "r" (cache->toPtr(i)): "memory");
            }
            cache->forgetThread();
        });
    }
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> sec = end - start;
    std::cout << (sec.count() / (N*nthreads) / 1e-9) << std::endl;
    return 0;
}
