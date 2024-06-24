#include <chrono>
#include <string.h>
#include <stdint.h>
#include <iostream>
#include <vector>
#include <drivers/nvme.hh>

using namespace std;

inline uint64_t rdtsc(void){
    union {
        uint64_t val;
        struct {
            uint32_t lo;
            uint32_t hi;
        };
    } tsc;
    asm volatile ("rdtsc" : "=a" (tsc.lo), "=d" (tsc.hi));
    return tsc.val;
}

int main(int argc, char** argv){
    unsigned datasize = 4096, blockSize=512;
    const unvme_ns_t* ns = unvme_open();
    while(!ns){
        printf("nvme error\n");
        ns = unvme_open();
    }
    printf("ns opened\n");
    char* b = (char*)unvme_alloc(ns, datasize);
    for(int i=0; i<datasize; i++){
        char d = (char)(i*i)%255;
        b[i] = d;
    }
    printf("buf alloc-ed and populated\n");
    u64 start = rdtsc();
    for(int i=0; i<datasize; i++){
        while(unvme_write(ns, 0, b, i, datasize/blockSize) != 0){
            printf("nvme error\n");
        }
    }
    u64 elapsed = rdtsc() - start;
    std::cout << "cycles per write: " << elapsed/datasize << std::endl;
    printf("write done\n");
    start = rdtsc();
    for(int i=0; i<datasize; i++){
        while(unvme_read(ns, 0, b, i, datasize/blockSize) != 0){
            printf("nvme error\n");
        }
    }
    elapsed = rdtsc() - start;
    std::cout << "cycles per read: " << elapsed/datasize << std::endl;
    printf("read done\n");
    return 0;
}
