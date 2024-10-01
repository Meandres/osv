#include <agent.hh>
#include <vector>
#include <stdio.h>   
#include <stdlib.h>
#include <osv/mmu.hh>
#include <sys/mman.h>
#include "helpers.hh"

using namespace std;

Agent *a;

void write(void* mem, size_t size){
    a->func(mem, size);
}

int main(){
    std::vector<external_function_t> helpers {reinterpret_cast<external_function_t>(&sync_write), reinterpret_cast<external_function_t>(&async_write), reinterpret_cast<external_function_t>(&poll_completion)};
    int sizeofblock = 4096, value=42;
    char* block = (char*)mmap(NULL, sizeofblock*sizeof(char), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    memcpy(block, &value, sizeof(int));
    a = new Agent("ebpf_prog.o", "write_1", helpers);
    write((void*)block, sizeofblock);
    a->reload("ebpf_prog.o", "write_2");
    value++;
    memcpy(block, &value, sizeof(int));
    write((void*)block, sizeofblock);
    return 0;
}
