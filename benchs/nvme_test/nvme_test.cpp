#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h> 
#include <stdlib.h>
#ifdef OSV
#include "drivers/nvme.hh"
#endif 
#include <iostream>

using namespace std;
static inline uint64_t rdtsc(void)
{
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

int main(){	
	const unsigned datasize=4096;
	const uint64_t runfor = 40;
	uint64_t lat_avg = 0;
	uint64_t io = 0;
	#ifdef OSV
	const unvme_ns_t* ns = unvme_open();
	if(!ns) exit(1);
	//void* buf = unvme_alloc(ns, datasize);
	void* buf = malloc(datasize);
	uint64_t addr =(uint64_t)buf;
	#endif
	#ifdef LINUX
	int fd = open("/dev/nvme0n1", O_RDWR, O_DIRECT);
	void* buf = malloc(datasize);
	#endif
	uint64_t before, after;
	int maxIOs = 255;
	unvme_iod_t iod[maxIOs] = {};
	for(int i=0; i<runfor; i++){
		for(int j=0; j<maxIOs; j++){
			iod[j] = unvme_awrite(ns, 0, buf, (i*maxIOs + j)*8, 8);
			assert(iod[j]!=NULL);
		}
		for(int j=0; j<maxIOs; j++){
			int ret = unvme_apoll(iod[j], 1);
			assert(ret==0);
		}
	}
	cout << "Written" << endl;
	while( io < runfor*maxIOs){
		before = rdtsc();
		#ifdef OSV
		int ret = unvme_read(ns, 0, buf, io, 8);
		assert(ret==0);
		#endif
		#ifdef LINUX
		int ret = pread(fd, buf, datasize, 0);
		assert(ret==4096);
		#endif
		after=rdtsc();
		lat_avg+=after-before;
		io++;
	}
	lat_avg = lat_avg / io;
	cout << "Average number of cycle per request : " << lat_avg <<endl;
	return 0;
}
