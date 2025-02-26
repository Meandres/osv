#ifndef YMAP_HH
#define YMAP_HH

#include <atomic>
#include <vector>
#include "osv/types.h"
#include <osv/mmu.hh>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <sys/mman.h>
#include <cstring>
#include <bitset>
#include "drivers/rdtsc.h"
#include <osv/percpu.hh>
#include <osv/llfree.h>

constexpr uintptr_t get_mem_area_base(u64 area)
{
    return 0x400000000000 | uintptr_t(area) << 44;
}

extern u64 startPhysRegion;
extern u64 sizePhysRegion;
extern llfree_t* llfree_allocator;

static const u64 sizeSmallPage = 4096;
static const u64 sizeHugePage = 2 * 1024 * 1024;
static const u64 maxPageSize = sizeHugePage;

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

inline ulong read_cr4() {
    ulong r;
    asm volatile ("mov %%cr4, %0" : "=r"(r));
    return r;
}

inline void write_cr4(ulong r) {
    asm volatile ("mov %0, %%cr4" : : "r"(r)  : "memory");
}

inline void invalidateTLB() {
   write_cr3(read_cr3());
}

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

inline u64* ptepToPtr(u64 ptep) {
   return (u64*) (get_mem_area_base(0) | (ptep & pte_addr_mask(false)));
}

inline u64* getPTERef(void* virt) {
   u64 ptr = (u64)virt;
   u64 i0 = (ptr>>(12+9+9+9)) & 511;
   u64 i1 = (ptr>>(12+9+9)) & 511;
   u64 i2 = (ptr>>(12+9)) & 511;
   u64 i3 = (ptr>>(12)) & 511;

   u64 l0 = ptepToPtr(ptRoot)[i0];
   u64 l1 = ptepToPtr(l0)[i1];
   u64 l2 = ptepToPtr(l1)[i2];
   return reinterpret_cast<u64*>(&ptepToPtr(l2)[i3]);
}

inline std::atomic<u64>* walkRef(void* virt) {
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

inline PTE walk(void* virt) {
   PTE pte = PTE(*walkRef(virt));
   return pte;
}

inline u64* ptepToPtrHuge(u64 ptep) {
   return (u64*) (get_mem_area_base(0) | (ptep & pte_addr_mask(true)));
}

inline std::atomic<u64>* walkRefHuge(void* virt) {
   u64 ptr = (u64)virt;
   u64 i0 = (ptr>>(12+9+9+9)) & 511;
   u64 i1 = (ptr>>(12+9+9)) & 511;
   u64 i2 = (ptr>>(12+9)) & 511;

   u64 l0 = ptepToPtr(ptRoot)[i0];
   u64 l1 = ptepToPtr(l0)[i1];
   return reinterpret_cast<std::atomic<u64>*>(&ptepToPtr(l1)[i2]);
}

inline PTE walkHuge(void* virt) {
   PTE pte = PTE(*walkRefHuge(virt));
   assert(pte.huge_page_null == 1);
   return pte;
}

inline bool trySetClean(void* addr){
   std::atomic<u64>* pteRef = walkRef(addr);
   u64 oldWord = PTE(*pteRef).word;
   PTE newPTE = PTE(oldWord);
   if(newPTE.dirty == 0){
      return false;
   }
   newPTE.dirty = 0;
   return pteRef->compare_exchange_strong(oldWord, newPTE.word);
}

/* Possible transitions
 * Mapped -> SoftUnmapped (with trySoftUnmap()) races with itself
 * SoftUnmapped -> Mapped (with tryHandlingSoftFault()) races with itself and SU->HU
 * SoftUnmapped -> HardUnmapped (with tryUnmapPhys()) races with itself and SU->M
 * HardUnmapped -> Mapped (with tryMapPhys()) races with itself
 */
enum BufferState{
   Mapped, // phys != 0 and present == 1
   SoftUnmapped, // phys != 0 and present == 0
   //CandidateEviction, // same as softunmapped + removed from resident set
   //Clean, // same as candidateeviction + synchronized with storage
   HardUnmapped, // phys == 0 and present == 0
   Inconsistent, // fail
   TBD // just something to work with
};

struct Buffer{
   std::atomic<u64>* pteRefs[16]; 
   PTE snapshot[16];
   BufferState snapshotState;
   void* baseVirt;
   int nb; // max 16 for 64KiB max size (+ 2MiB)
   // overhead (max for buffer size == sizeSmallPage) => 8+8+4+8+4 = 32B = 0.78%

   Buffer(void* addr, u64 size);
   ~Buffer(){}

   PTE at(int index) { return snapshot[index]; }

   PTE basePTE(){
      return snapshot[0];
   }

   void updateSnapshot(){
      snapshotState = BufferState::TBD;
      for(int i = 0; i < nb; i++){
         snapshot[i] = PTE(*pteRefs[i]);
         if(snapshotState == BufferState::Inconsistent){
            continue; // skip since we already reached an inconsistent state
         }
         if(snapshot[i].phys != 0){
            if(snapshot[i].present == 1){ // phys != 0 && present == 1
               if(snapshotState == BufferState::Mapped || snapshotState == BufferState::TBD)
                  snapshotState = BufferState::Mapped;
               else
                  snapshotState = BufferState::Inconsistent; 
            }else{ // phys != 0 && present == 0
               if(snapshotState == BufferState::SoftUnmapped || snapshotState == BufferState::TBD)
                  snapshotState = BufferState::SoftUnmapped;
               else
                  if(snapshotState != BufferState::HardUnmapped){ // phys != 0 && present == 0 is inconsistent only for the first page
                             // since we only remove the frame address for the first page
                     snapshotState = BufferState::Inconsistent;
                  }
            }
         }else{
            if(snapshot[i].present == 0){ // phys == 0 && present == 0
               if(snapshotState == BufferState::HardUnmapped || snapshotState == BufferState::TBD)
                  snapshotState = BufferState::HardUnmapped;
               else
                  snapshotState = BufferState::Inconsistent;
            }else // phys == 0 && present == 1
               snapshotState = BufferState::Inconsistent;
         }
      }
   }

   void tryClearAccessed();

   bool tryHandlingSoftFault();
   bool trySoftUnmap();

   bool tryMapPhys(u64 phys);
   u64 tryUnmapPhys();

   int getPresent();
   int getAccessed();
   u16 getDirty();

   // for non-concurrent scenarios
   void map(u64 phys);
   u64 unmap();
   void invalidateTLBEntries();

};

void initYmaps();
u64 ymap_getPage(int order);
void ymap_putPage(u64 phys, int order);
#endif
