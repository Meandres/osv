#include <iostream>
#include "processor.hh"
/*#include <sys/time.h>

using namespace std;

static double gettime(void) {
   struct timeval now_tv;
   gettimeofday (&now_tv,NULL);
   return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec)/1000000.0;
}*/

/*static inline void wrmsr(uint64_t msr, uint64_t value)
{
    uint32_t low = value & 0xFFFFFFFF;
    uint32_t high = value >> 32;
    asm volatile (
        "wrmsr"
        :
        : "c"(msr), "a"(low), "d"(high)
    );
}

static inline uint64_t rdmsr(uint64_t msr)
{
    uint32_t low, high;
    asm volatile (
        "rdmsr"
        : "=a"(low), "=d"(high)
        : "c"(msr)
    );
	return ((uint64_t)high << 32) | low;
}*/

#define XCR_XFEATURE_ENABLED_MASK   0x00000000
#define XCR_XFEATURE_VALUE 0x00000000000002e7 // this is what Liunx set (on irene)

static inline void xsetbv(uint32_t index, uint64_t value)
{
        uint32_t eax = value;
        uint32_t edx = value >> 32;

        asm volatile("xsetbv" :: "a" (eax), "d" (edx), "c" (index));
}

int main()
{
    /*rdmsrl(MSR_MTRRdefType, msr_deftype);
    printf("MTRRdefType=%lx\n", msr_deftype);
    rdmsr(MSR_MTRRcap, msr_cap);
    printf("MTRRcap=%lx\n", msr_cap);
    vcnt = (int)(msr_cap & MTRR_VCNT_MASK);
    for (idx = 0; idx < vcnt; idx += 1) {
        rdmsr(MSR_IA32_MTRR_PHYSBASE(idx), base);
        rdmsr(MSR_IA32_MTRR_PHYSMASK(idx), mask);
        if (!(mask & MTRR_VRRP_MASK_MASK)) continue;
        start_mfn = base >> 12;
        num_mfn = (~(mask >> 12) & ((1UL << (phys_addr_size - 12)) - 1)) + 1;
        mem_type = base & 0xff;
        printf("===MTTR base=%lx mask=%lx start=%lx num_mfn=%d type=%d\n",
                base, mask, start_mfn, (int)num_mfn, mem_type);
    }
    return 0;*/
    asm volatile("xsetbv" :: "a" (0x207), "c" (0), "d" (0));
    printf("xcr0: %lu\n", processor::read_xcr(processor::xcr0));
    for (uint64_t j=0; j<3000; j++) {
         //double start = gettime();
         for (uint64_t jj=0; jj<1000; jj++) {
            asm volatile("addq $42, %%rax": : : "memory");
         }
         //cout << gettime()-start << endl;
      }

      return 0;
}
