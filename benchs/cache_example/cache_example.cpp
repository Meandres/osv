#include <osv/cache.hh>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include "drivers/rdtsc.h"

using namespace std;

int main(int argc, char** argv){
    int size = 20;
    u64 virtSize = size * 4096;
    u64 physSize = size * 4096;
    
    createCache(physSize, 2);
    uCacheManager->addVMA(virtSize, 4096);
    void* mem = uCacheManager->vmaTree->vma->start;
    std::vector<u64> toFlush;
    
    for(int i=0; i<size; i++){
        Buffer buffer(mem+(i*4096), 4096);
        if(!buffer.tryMapPhys(ymap_getPage(computeOrder(4096)))){
            printf("error map\n");
            crash_osv();
        }
        memcpy(mem+(i*4096), &i, sizeof(int));
        toFlush.push_back(i);
    }

    uCacheManager->flush(toFlush);     

    void* buf = mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    Buffer buffer(buf, 4096);
    buffer.tryMapPhys(ymap_getPage(computeOrder(4096)));
    u64 val;
    for(int i=0; i<size; i++){
        unvme_read(uCacheManager->ns, 0, buf, i, 1);
        memcpy(&val, buf, sizeof(int));
        printf("val: %u\n", val);
    }
    return 0;
}
