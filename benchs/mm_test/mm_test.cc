#include <string.h>
#include <stdint.h>
#include <iostream>
#include <cstdlib>
#include <experimental/random>
#include <ctime>
#include <sys/mman.h>

using namespace std;

inline uint64_t rdtsc(void){
    union{
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
    if(argc != 2){
        cerr << "Please provide the system that is running the bench" << endl;
        return 1;
    }
    const uint64_t nb_copies = 100000000;
    const uint64_t nb_lookups = 1000000000;
    const int repetitions = 5;
    const int size = 16777216;
    char system[50];
    strcpy(system, argv[1]);
    srand(time(nullptr));
    void* mem = mmap(NULL, size*sizeof(char), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    char* charMem = (char*) mem;
    char *dest = (char*) malloc(4096*sizeof(char));

    // load random values
    for(int i = 0; i<size; i++){
        char rnd_val = experimental::randint(0, 255);
        charMem[i] = rnd_val;
        if(i<4096)
            dest[i] = 0;
    }
    /*int pos;
    for(int cpySize=1; cpySize <= 4096; cpySize*=8){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            for(uint64_t j = 0; j<nb_copies; j++){
                pos = experimental::randint(0, size-cpySize)%(size-cpySize);
                memcpy(dest, charMem+pos, cpySize);
            }
            uint64_t end = rdtsc();
            cout << cpySize << "," << system << "," << (double)(end-start)/nb_copies << endl;
        }
    }*/
    int pos;
    uint64_t acc = 0;
    uint64_t start = rdtsc();
    for(uint64_t i=0; i<nb_lookups; i++){
        pos = experimental::randint(0, size);
        acc += (uint64_t)charMem[pos];
    }
    uint64_t end = rdtsc();
    cout << system << "," << (double)(end-start)/nb_lookups << endl;
    
    /*cout << "Average cycles for memcpy_small " << ((double)cycles_memcpy_small)/nb_memcpy_small << ", count: " << nb_memcpy_small << endl;
    cout << "Average cycles for memcpy_ssse3 " << ((double)cycles_memcpy_ssse3)/nb_memcpy_ssse3 << ", count: " << nb_memcpy_ssse3 << endl;
    cout << "Average cycles for memcpy_ssse3_unal " << ((double)cycles_memcpy_ssse3_unal)/nb_memcpy_ssse3_unal << ", count: " << nb_memcpy_ssse3_unal << endl;
    cout << "Average cycles for memcpy_repmovsb " << ((double)cycles_memcpy_repmovsb)/nb_memcpy_repmovsb << ", count: " << nb_memcpy_repmovsb << endl;*/
    return 0;
}
