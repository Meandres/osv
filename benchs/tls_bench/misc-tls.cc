/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

// Benchmark the speed overhead of using a thread-local variables in an
// application. The normal overhead of a thread-local variable in a Linux
// application is very small, because the application is compiled as an
// executable, the "initial exec" TLS model is used, where the variable's
// address is computed with compile-time-generated code. The overhead in
// OSv applications, compiled as shared libraries, are higher because it
// uses the "global dynamic" TLS model and every TLS access becomes a call
// to the __tls_get_addr function - but here we want to test just how slow
// it is.

#include <chrono>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#ifdef OSV
#include <osv/cache.hh>
#endif

__thread int var_tls __attribute__ ((tls_model ("initial-exec"))) = 0;
int var_global;

int stick_this_thread_to_core(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

template<class Fn>
void parallel_for(uint64_t nthreads, Fn fn) {
    std::vector<std::thread> threads;
    for (unsigned i=0; i<nthreads; i++) {
        threads.emplace_back([&,i]() {
            stick_this_thread_to_core(i);
            fn(i);
        });
    }
    for (auto& t : threads)
        t.join();
}

uint64_t envOr(const char* env, uint64_t value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

int main()
{
    constexpr uint64_t N = 100000000;

    uint64_t nthreads = envOr("THREADS", 1);
        auto start = std::chrono::system_clock::now();
        parallel_for(nthreads, [&](uint64_t worker) {
            int local_var;
            for (int i = 0; i < N; i++) {
                // To force gcc to not optimize this loop away
                asm volatile("" : : : "memory");
                ++var_tls;
            }
        }); 
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> sec = end - start;
        std::cout << (sec.count() / N / 1e-9) << "\n";

    #ifdef OSV
    start = std::chrono::system_clock::now();
    parallel_for(nthreads, [&](uint64_t worker) {
        for (int i = 0; i < N; i++) {
            // To force gcc to not optimize this loop away
            asm volatile("" : : : "memory");
            ++tls_in_kernel;
        }
    }); 
    end = std::chrono::system_clock::now();
    sec = end - start;
    std::cout << (sec.count() / N / 1e-9) << "\n";
    #endif

    start = std::chrono::system_clock::now();
    parallel_for(nthreads, [&](uint64_t worker) {
        volatile int acc;
        for (int i = 0; i < N; i++) {
            asm volatile(""::: "memory");
            acc = std::hash<std::thread::id>(std::this_thread::get_id());
        }
    }); 
    end = std::chrono::system_clock::now();
    sec = end - start;
    std::cout << (sec.count() / N / 1e-9) << "\n";

    return 0;
    
    // 4, 8, 16, 32 and 64 threads
    for (int nthreads=4; nthreads<=64; nthreads*=2){
        std::cout << nthreads << ";";
        start = std::chrono::system_clock::now();
        parallel_for(nthreads, [&](uint64_t worker) {
            int local_var;
            for (int i = 0; i < N; i++) {
                // To force gcc to not optimize this loop away
                asm volatile("" : : : "memory");
                ++local_var;
            }
        }); 
        end = std::chrono::system_clock::now();
        sec = end - start;
        std::cout << (sec.count() / N / 1e-9) << ";";

        start = std::chrono::system_clock::now();
        parallel_for(nthreads, [&](uint64_t worker) {
            for (int i = 0; i < N; i++) {
                // To force gcc to not optimize this loop away
                asm volatile("" : : : "memory");
                ++var_tls;
            }
        }); 
        end = std::chrono::system_clock::now();
        sec = end - start;
        std::cout << (sec.count() / N / 1e-9) << "\n";
    }
    std::cout << std::endl;
    return 0;
}
