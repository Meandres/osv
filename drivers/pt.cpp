#include <atomic>

constexpr uintptr_t get_mem_area_base(u64 area)
{
    return 0x400000000000 | uintptr_t(area) << 44;
}

/*Page* phys_to_virt(u64 phys) {
   return ((Page*)get_mem_area_base(0)) + phys;
   }*/

constexpr int page_size_shift = 12; // log2(page_size)
constexpr uint8_t rsvd_bits_used = 1;
constexpr uint8_t max_phys_bits = 52 - rsvd_bits_used;

inline void invalidateTLBEntry(void* addr) {
   asm volatile("invlpg (%0)" ::"r" (addr) : "memory");
}

inline ulong read_cr3() {
    ulong r;
    asm volatile ("mov %%cr3, %0" : "=r"(r));
    return r;
}

inline void write_cr3(ulong r) {
    asm volatile ("mov %0, %%cr3" : : "r"(r)  : "memory");
}

inline void invalidateTLB() {
   write_cr3(read_cr3());
}

inline void __invpcid(unsigned pcid, void* addr, unsigned type) {
   struct { u64 d; void* p; } desc = { pcid, addr };
   asm volatile("invpcid %[desc], %[type]" :: [desc] "m" (desc), [type] "r" (type) : "memory");
}

inline void invpcidFlushOne(unsigned pcid, void* addr) { __invpcid(pcid, addr, 0); }
inline void invpcidFlushContext(unsigned pcid) { __invpcid(pcid, 0, 1); }
inline void invpcidFlushAll() { __invpcid(0, 0, 2); }
inline void invpcidFlushAllNonGlobal() { __invpcid(0, 0, 3); }

constexpr uint64_t pte_addr_mask(bool large = false) {
    return ((1ull << max_phys_bits) - 1) & ~(0xfffull) & ~(uint64_t(large) << page_size_shift);
}

static u64 ptRoot = read_cr3();

struct PTE {
   union {
      u64 word;
      struct {
         u64 present : 1; // the page is currently in memory
         u64 writable : 1; // it's allowed to write to this page
         u64 user : 1; // accessible if not set, only kernel mode code can access this page
         u64 write : 1; // through caching writes go directly to memory
         u64 disable_cache : 1; // no cache is used for this page
         u64 accessed : 1; // the CPU sets this bit when this page is used
         u64 dirty : 1; // the CPU sets this bit when a write to this page occurs
         u64 huge_page_null : 1; // must be 0 in P1 and P4, creates a 1GiB page in P3, creates a 2MiB page in P2
         u64 global_page : 1; // isn't flushed from caches on address space switch (PGE bit of CR4 register must be set)
         u64 unused : 3; // available can be used freely by the OS
         u64 phys : 40; // physical address the page aligned 52bit physical address of the frame or the next page table
         u64 unused2 : 11; // available can be used freely by the OS
         u64 no_execute : 1; // forbid executing code on this page (the NXE bit in the EFER register must be set)
      };
   };

   PTE() {}
   PTE(u64 word) : word(word) {}

};

u64* ptepToPtr(u64 ptep) {
   return (u64*) (get_mem_area_base(0) | (ptep & pte_addr_mask(false)));
}

std::atomic<u64>* walkRef(void* virt) {
   u64 ptr = (u64)virt;
   u64 i0 = (ptr>>(12+9+9+9)) & 511;
   u64 i1 = (ptr>>(12+9+9)) & 511;
   u64 i2 = (ptr>>(12+9)) & 511;
   u64 i3 = (ptr>>(12)) & 511;

   u64 l0 = ptepToPtr(ptRoot)[i0];
   u64 l1 = ptepToPtr(l0)[i1];
   u64 l2 = ptepToPtr(l1)[i2];
   return reinterpret_cast<std::atomic<u64>*>(&ptepToPtr(l2)[i3]);
}

PTE walk(void* virt) {
   return PTE(*walkRef(virt));
}

u64* ptepToPtrHuge(u64 ptep) {
   return (u64*) (get_mem_area_base(0) | (ptep & pte_addr_mask(true)));
}

u64* walkRefHuge(void* virt) {
   u64 ptr = (u64)virt;
   u64 i0 = (ptr>>(12+9+9+9)) & 511;
   u64 i1 = (ptr>>(12+9+9)) & 511;
   u64 i2 = (ptr>>(12+9)) & 511;

   u64 l0 = ptepToPtr(ptRoot)[i0];
   u64 l1 = ptepToPtr(l0)[i1];
   return &ptepToPtr(l1)[i2];
}

PTE walkHuge(void* virt) {
   PTE pte = *walkRefHuge(virt);
   assert(pte.huge_page_null == 1);
   return pte;
}
