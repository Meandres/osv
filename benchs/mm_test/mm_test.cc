#include <chrono>
#include <string.h>
#include <stdint.h>
#include <iostream>
#include <vector>

#ifdef OSV
#include <osv/memcmp.h>
#endif

//extern "C" int fast_memcmp(const void*, const void*, size_t);

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

int sanity_check(const void *vl, const void *vr, size_t n)
{
	const unsigned char *l=(const unsigned char*)vl, *r=(const unsigned char*)vr;
	for (; n && *l == *r; n--, l++, r++);
	return n ? *l-*r : 0;
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

    const uint64_t nb_op = 1000000000;
    const int repetitions = 1;
    char system[50];
    strcpy(system, argv[1]);
    char *og = (char*) malloc(4096*sizeof(char));
    put_things_in_string(og, 4096);
    char *dest = (char*) malloc(4096*sizeof(char));
    vector<int> sizes_memcpy = {0, 1, 2, 4, 8, 16, 32, 64, 72, 96, 128, 256, 512, 768, 4088};
    vector<int> sizes_memcmp = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 41};

    uint64_t acc_time=0;
    /*for(int cpySize: sizes_memcpy){
        for(int i=0; i<repetitions; i++){
            auto start = std::chrono::system_clock::now();
            for(uint64_t j = 0; j<nb_op; j++){
                memcpy(dest, og, cpySize);
            }
            auto stop = std::chrono::system_clock::now();
            std::chrono::duration<double> sec = stop - start;
            acc_time += sec.count();
        }
        cout << "memcpy," << cpySize << "," << system << "," << (double)(acc_time/repetitions)/nb_op/1e-9 << endl;
    }*/

    int acc = 0;
    /*for(int i=0; i<repetitions; i++){
        uint64_t start = rdtsc();
        //auto start = std::chrono::system_clock::now();
        for(uint64_t j = 0; j<nb_op; j++){
            acc += og[j%4096];
        }
        uint64_t stop = rdtsc();
        //auto stop = std::chrono::system_clock::now();
        //std::chrono::duration<double> sec = stop - start;
        //acc_time += sec.count();
        acc_time += (stop-start);
    }
    //cout << "access," << system << "," << (double)(acc_time/repetitions)/nb_op/1e-9 << endl;
    cout << "access," << system << "," << ((double)acc_time)/(repetitions*nb_op) << endl;*/
     
    // this is the worst case in OSv since it just compares sequentially
    put_things_in_string(og, 4096);
    put_things_in_string(dest, 4096);
    acc_time=0.0;
    acc = 0;
    for(int cmpSize: sizes_memcmp){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            //auto start = chrono::system_clock::now();
            for(volatile uint64_t j = 0; j<nb_op; j++){
                acc += memcmp(dest, og, cmpSize);
            }
            //auto stop = chrono::system_clock::now();
            //chrono::duration<double> sec = stop - start;
            //acc_time += sec.count();
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        //cout << "memcmp," << cmpSize << "," << system << "," << (double)(acc_time/repetitions)/nb_op/1e-9 << ",\t" <<acc << endl;
        if(strcmp(system, "linux") == 0)
            cout << "glibc," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc <<endl;
        else
            cout << "musl," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc <<endl;
    }
   
    #ifdef LINUX
    acc_time=0.0;
    acc = 0;
    for(int cmpSize: sizes_memcmp){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            //auto start = chrono::system_clock::now();
            for(volatile uint64_t j = 0; j<nb_op; j++){
                acc += sanity_check(dest, og, cmpSize);
            }
            //auto stop = chrono::system_clock::now();
            //chrono::duration<double> sec = stop - start;
            //acc_time += sec.count();
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        //cout << "musl on linux," << cmpSize << "," << system << "," << (double)(acc_time/repetitions)/nb_op/1e-9 << ",\t" <<acc << endl;
        cout << "musl," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc << endl;
    }
    #endif
    
    #ifdef OSV
    acc_time=0.0;
    acc = 0;
    for(int cmpSize: sizes_memcmp){
        for(int i=0; i<repetitions; i++){
            uint64_t start = rdtsc();
            //auto start = chrono::system_clock::now();
            for(volatile uint64_t j = 0; j<nb_op; j++){
                acc += fast_memcmp(dest, og, cmpSize);
            }
            //auto stop = chrono::system_clock::now();
            //chrono::duration<double> sec = stop - start;
            //acc_time += sec.count();
            uint64_t stop = rdtsc();
            acc_time += (stop-start);
        }
        //cout << "glibc on osv," << cmpSize << "," << system << "," << (double)(acc_time/repetitions)/nb_op/1e-9 << ",\t" <<acc << endl;
        cout << "glibc," << cmpSize << "," << system << "," << static_cast<double>(acc_time)/(repetitions*nb_op) << "," << acc << endl;
    }
    #endif
    return 0;
}
