#include <chrono>
#include <string.h>
#include <stdint.h>
#include <iostream>
#include <vector>
#include "rte_string.hh"

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

void put_things_in_string(char* str, size_t n){
    for(int i=0; i<n; i++){
        char c = i%10;
        str[i] = c;
    }
    str[n-1] = '\0';
}

int main(int argc, char** argv){
    if(argc != 2){
        cerr << "Please provide the system that is running the bench" << endl;
        return 1;
    }

    const uint64_t nb_op = 100000000;
    const int repetitions = 3;
    char system[50];
    strcpy(system, argv[1]);
    char *og = (char*) malloc(4096*sizeof(char));
    put_things_in_string(og, 4096);
    char *dest = (char*) malloc(4096*sizeof(char));
    memset(og, 0, 4096*sizeof(char));
    memset(dest, 0, 4096*sizeof(char));
    vector<int> sizes_memcpy = {0, 1, 2, 4, 8, 16, 32, 64, 72, 96, 128, 256, 512, 768, 4088, 4096};
    vector<int> sizes_memcmp = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 41};

    uint64_t acc_time=0;
    for(int cpySize: sizes_memcpy){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            for(uint64_t j = 0; j<nb_op; j++){
                memcpy(dest, og, cpySize);
            }
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        cout << "memcpy," << cpySize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << endl;
    }
    acc_time=0;
    for(int cpySize: sizes_memcpy){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            for(uint64_t j = 0; j<nb_op; j++){
                rte_memcpy(dest, og, cpySize);
            }
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        cout << "rte_memcpy," << cpySize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << endl;
    }

    int acc = 0;
    for(int cmpSize: sizes_memcmp){
        acc_time=0;
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            for(volatile uint64_t j = 0; j<nb_op; j++){
                acc += memcmp(dest, og, cmpSize);
            }
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        cout << "memcmp," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc << endl;
    }
    // this is the worst case in OSv since it just compares sequentially
    put_things_in_string(og, 4096);
    put_things_in_string(dest, 4096);
    acc = 0;
    for(int cmpSize: sizes_memcmp){
        acc_time=0;
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            for(volatile uint64_t j = 0; j<nb_op; j++){
                acc += rte_memcmp(dest, og, cmpSize);
            }
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        cout << "rte_memcmp," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc << endl;
    }
    
    return 0;
}
