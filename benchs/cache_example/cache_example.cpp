#include <osv/cache.hh>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <random>
#include <functional>
#include "BTree.hpp"
#include "drivers/rdtsc.h"

std::uniform_int_distribution<u64> int_distri[32];
std::mt19937 engine[32];

CacheManager* cache;
using namespace std;

const int intPerPage = 4096/sizeof(int);

u64 rng(int tid){
    return int_distri[tid](engine[tid]);
}

void generateRandPage(int tid, u64 pid){
    int n;
    GuardX<Page> node(pid);
    for(int i=0; i<intPerPage; i++){
        n = rng(tid);
        memcpy((void*)(node.ptr)+i*sizeof(int), &n, sizeof(int));
    }
}

void updateRandomInt(int tid, void *start, u64 pageCount, int pid){
    u64 page = rng(tid)%pageCount; 
    int posInPage = rng(tid)%intPerPage;
    int increment = rng(tid);
    GuardX<Page> node(page);
    int content;
    void* ptr = (void*)(node.ptr) + posInPage*sizeof(int);
    memcpy(&content, ptr, sizeof(int));
    content += increment;
    memcpy(ptr, &content, sizeof(int));
}

template<class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
   std::vector<std::thread> threads;
   uint64_t n = end-begin;
   if (n<nthreads)
      nthreads = n;
   uint64_t perThread = n/nthreads;
   for (unsigned i=0; i<nthreads; i++) {
      threads.emplace_back([&,i]() {
         uint64_t b = (perThread*i) + begin;
         uint64_t e = (i==(nthreads-1)) ? end : ((b+perThread) + begin);
         fn(i, b, e);
      });
   }
   for (auto& t : threads)
      t.join();
}

void loadArray(void* start, u64 size, int n_threads){
    parallel_for(0, size, n_threads, [&](uint64_t worker, uint64_t begin, uint64_t end){
        std::uniform_int_distribution<u64> int_distribution(1, 1ull<<63);
        std::mt19937 random_number_engine;
        int_distri[worker] = int_distribution;
        engine[worker] = random_number_engine;
        cache->registerThread();
        for(u64 i=begin; i<end; i++){
            generateRandPage(worker, i);
        }
        cache->forgetThread();
        add_thread_results();
    });
}

u64 envOr(const char* env, u64 value){
    if (getenv(env))
        return atof(getenv(env));
    return value;
}

int main(int argc, char** argv){
    u64 virtSize = envOr("VIRTGB", 2ull) * 1024 * 1024 * 1024;
    u64 physSize = envOr("PHYSGB", 2ull) * 1024 * 1024 * 1024;
    //u64 virtSize = 32768 * 4096;
    //u64 physSize = 32768 * 4096;
    int n_threads=envOr("THREADS", 1);
    u64 statDiff = 1e8;
    atomic<u64> txProgress(0);
    atomic<bool> keepRunning(true);
    u64 runForSec = envOr("RUNFOR", 30);

    auto statFn = [&]() {
        cout << "ts,tx,rmb,wmb,pageFaults,threads" << endl;
        u64 cnt = 0;
        for (uint64_t i=0; i<runForSec; i++) {
            sleep(1);
            float rmb = (cache->readCount.exchange(0)*pageSize)/(1024.0*1024);
            float wmb = (cache->writeCount.exchange(0)*pageSize)/(1024.0*1024);
            u64 prog = txProgress.exchange(0);
            u64 pfCount = pageFaultNumber.exchange(0);
            cout << cnt++ << "," << prog << "," << rmb << "," << wmb <<  "," << pfCount << "," << n_threads << endl;
        }
        keepRunning = false;
    };
    cache = createMMIORegion(NULL, virtSize, physSize, n_threads, 64, false);
    if(cache->explicit_control)
        cout << "explicit_control " << endl;
    loadArray(cache->virtMem, virtSize/4096, n_threads);
    cache->readCount = 0;
    cache->writeCount = 0;
    pageFaultNumber = 0;
    thread statThread(statFn);

    parallel_for(0, n_threads, n_threads, [&](uint64_t worker, uint64_t begin, uint64_t end){
        cache->registerThread();
        u64 cnt = 0;
        u64 start = rdtsc();
        while(keepRunning.load()){
            updateRandomInt(worker, cache->virtMem, virtSize/4096, i);
            cnt++;
            u64 stop = rdtsc();
            if((stop-start) > statDiff){
                txProgress += cnt;
                start = stop;
                cnt = 0;
            }
        }
        txProgress += cnt;
        cache->forgetThread();
        add_thread_results();
    });

    statThread.join();
    print_aggregate_avg();
    return 0;
}

/*
int main(int argc, char** argv){
    u64 pageSize = 4096;
    u64 virtCount = 100;
    u64 physCount = 50;
    CacheManager* cache = createMMIORegion(NULL, virtCount*pageSize, physCount*pageSize, 1, 10, false);
    void* region = cache->virtMem;
    cout << "Region : "<< region << endl;
    cache->registerThread();
    cache->fixX(0); // simply marks the page as locked
                    // it is up to the application to use it as it sees fit
    for(int i=0; i<virtCount; i++){
	    memset(region+i*pageSize, 0, pageSize);
	    int* ptr = (int*)(region+i*pageSize);
       	*ptr = (int)i+1;
    }
    for(int i=0; i<virtCount; i++){
    	cout << "Nb: " << *(int*)(region+i*pageSize) << endl;
    }

    cache->forgetThread();
}*/
