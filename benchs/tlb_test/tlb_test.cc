#include <chrono>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#ifdef OSV
#include <osv/cache.hh>
#include <osv/sampler.hh>
#endif
#include <sys/mman.h>

using namespace std;

int stick_this_thread_to_core(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

void* page;

void worker(){
    stick_this_thread_to_core(3);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    {
        PTE pte = walk(page);
        printf("worker before access -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    memset(page, 1, 4096);
    {
        PTE pte = walk(page);
        printf("worker after access -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        PTE pte = walk(page);
        printf("worker after bits cleared by main -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    memset(page, 3, 64);
    //invalidateTLBEntry(page);
    {
        PTE pte = walk(page);
        printf("worker after access after clearing bits -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
}

int main(int argc, char* argv[])
{
    stick_this_thread_to_core(2);
    page = mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0); 
    std::thread wo(worker);
    {
        PTE pte = walk(page);
        printf("main before access -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    memset(page, 2, 4096);
    {
        PTE pte = walk(page);
        printf("main after access -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        std::atomic<u64>* ptePtr = walkRef(page);
        PTE with_accessed = (PTE)*ptePtr;
        with_accessed.accessed = 0;
        with_accessed.dirty = 0;
        ptePtr->store(with_accessed.word);
        invalidateTLBEntry(page);
        PTE pte = walk(page);
        printf("main after clearing both bits -- accessed is %i, dirty is %i\n", pte.accessed, pte.dirty);
    }
    wo.join();
    return 0;
}
