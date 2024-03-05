#include <string.h>
#include <stdint.h>
#include <iostream>
#include <cstdlib>
#include <experimental/random>
#include <ctime>

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
    const int repetitions = 5;
    char system[50];
    strcpy(system, argv[1]);
    srand(time(nullptr));
    char array_null[4096] = {};
    char *array_1 = (char*) malloc(1*sizeof(char));
    char *array_8 = (char*) malloc(8*sizeof(char));
    char *array_512 = (char*) malloc(512*sizeof(char));
    char *array_4096 = (char*) malloc(4096*sizeof(char));
    char *dest_1 = (char*) malloc(1*sizeof(char));
    char *dest_8 = (char*) malloc(8*sizeof(char));
    char *dest_512 = (char*) malloc(512*sizeof(char));
    char *dest_4096 = (char*) malloc(4096*sizeof(char));

    // load random values
    for(int i = 0; i<4096; i++){
        char rnd_val = experimental::randint(0, 255);
        if(i<1){
           array_1[i] = rnd_val; 
        }
        if(i<8){
           array_8[i] = rnd_val; 
        }
        if(i<512){
           array_512[i] = rnd_val; 
        }
        array_4096[i] = rnd_val;
    }

    for(int i=0; i<repetitions; i++){
        uint64_t start = rdtsc();
        for(uint64_t j = 0; j<nb_copies; j++){
            array_1[0] = (array_1[0] + j)%255;
            memcpy(dest_1, array_1, 1);
        }
        uint64_t end = rdtsc();
        cout << "1," << system << "," << (double)(end-start)/nb_copies << endl;
    }
    
    for(int i=0; i<repetitions; i++){
        uint64_t start = rdtsc();
        for(uint64_t j = 0; j<nb_copies; j++){
            array_8[j%8] = (array_8[j%8] + j)%255;
            memcpy(dest_8, array_8, 8);
        }
        uint64_t end = rdtsc();
        cout << "8," << system << "," << (double)(end-start)/nb_copies << endl;
    }

    for(int i=0; i<repetitions; i++){
        uint64_t start = rdtsc();
        for(uint64_t j = 0; j<nb_copies; j++){
            array_512[j%512] = (array_512[j%512] + j)%255;
            memcpy(dest_512, array_512, 512);
        }
        uint64_t end = rdtsc();
        cout << "512," << system << "," << (double)(end-start)/nb_copies << endl;
    }

    for(int i=0; i<repetitions; i++){
        uint64_t start = rdtsc();
        for(uint64_t j = 0; j<nb_copies; j++){
            array_4096[j%4096] = (array_4096[j%4096] + j)%255;
            memcpy(dest_4096, array_4096, 4096);
        }
        uint64_t end = rdtsc();
        cout << "4096," << system << "," << (double)(end-start)/nb_copies << endl;
    }
    /*cout << "Average cycles for memcpy_small " << ((double)cycles_memcpy_small)/nb_memcpy_small << ", count: " << nb_memcpy_small << endl;
    cout << "Average cycles for memcpy_ssse3 " << ((double)cycles_memcpy_ssse3)/nb_memcpy_ssse3 << ", count: " << nb_memcpy_ssse3 << endl;
    cout << "Average cycles for memcpy_ssse3_unal " << ((double)cycles_memcpy_ssse3_unal)/nb_memcpy_ssse3_unal << ", count: " << nb_memcpy_ssse3_unal << endl;
    cout << "Average cycles for memcpy_repmovsb " << ((double)cycles_memcpy_repmovsb)/nb_memcpy_repmovsb << ", count: " << nb_memcpy_repmovsb << endl;*/

    return 0;
}
