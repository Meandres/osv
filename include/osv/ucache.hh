#ifndef UCACHE_HH
#define UCACHE_HH

#include <osv/mmu.hh>
#include <atomic>
#include <vector>
#include <algorithm>
#include <tuple>
#include <cassert>
#include <iostream>
#include <unistd.h>
#include <immintrin.h>
#include <cmath>
#include <cstring>
#include <bitset>
#include <sys/mman.h>

#include <osv/types.h>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <osv/percpu.hh>
#include <osv/llfree.h>
#include <osv/power.hh>
#include <drivers/poll_nvme.hh>

static const u64 maxPageSize = mmu::huge_page_size;
#ifndef THRESHOLD_INVLPG_FLUSH
#define THRESHOLD_INVLPG_FLUSH 0
#endif

namespace ucache {

typedef u64 phys_addr;
typedef u64* virt_addr;

/// [ THREADSAFE ]
/// Get the VMA lock for the superblock containing addr
rwlock_t& vma_lock(const uintptr_t addr);

/// [ THREADSAFE ]
/// Get the Free Ranges lock for the superblock containing
rwlock_t& free_ranges_lock(const uintptr_t addr);

/// Get the VMA containing this address
boost::optional<mmu::vma*> find_intersecting_vma(const uintptr_t addr);

/// Get all VMAs from this range
std::vector<mmu::vma*> find_intersecting_vmas(const uintptr_t addr, const u64 size);

/// Insert a VMA into OSv's internal state
void insert(mmu::vma* v);

// Erase the VMA from OSv's internal state
void erase(mmu::vma* v);

/// [ THREADSAFE ]
/// Test if allocating the given region complies with OSv specific policies.
/// If this function returns positive it means allocation might succeed.
bool validate(const uintptr_t addr, const u64 size);

/// Allocate the given range in virtual memory. Non-validated ranges may fail
void allocate_range(const uintptr_t addr, const u64 size);

/// [ THREADSAFE ]
/// Reserves a virtual memory range of the given size
uintptr_t reserve_range(const u64 size, size_t alignment=mmu::page_size);

/// Free the given range by returning virtual memory back to OSv
void free_range(const uintptr_t addr, const u64 size);

/// [ THREADSAFE ]
/// Allocate physically contiguous memory of the specified order
/// Returns a virtual address from the linear mapping
void* frames_alloc(unsigned order);

phys_addr frames_alloc_phys_addr(size_t size);
void frames_free_phys_addr(u64 addr, size_t size);

/// [ THREADSAFE ]
/// Free physically contiguous memory
/// Requires the address returned by frames_alloc
void frames_free(void* addr, unsigned order);

/// [ THREADSAFE ]
/// Get amount of free physical memory
u64 stat_free_phys_mem();

/// [ THREADSAFE ]
/// Get total amount of physical memory
u64 stat_total_phys_mem();

struct pt_elem {
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

    pt_elem() {}
    pt_elem(u64 word) : word(word) {}
    static pt_elem make(u64 phys, bool huge){
        pt_elem pte;
        pte.word = 0ull;
        pte.writable = 1;
        if(huge){
           pte.huge_page_null = 1;
        }
        if(phys != 0){
            pte.present = 1;
            pte.phys = phys;
        }
        return pte;
    }
};

typedef pt_elem PTE;

static unsigned idx(void *virt, unsigned level)
{
    return ((u64)virt >> (12 + level * 9)) & 511;
}

inline virt_addr ensure_valid_pt_elem(virt_addr parent, unsigned idx, bool with_frame, bool huge=false){
    if(pt_elem(parent[idx]).phys == 0){ // if the children is an empty page
        u64 page_size = huge ? 2ul*1024*1024 : 4096;
        u64 frame = with_frame ? frames_alloc_phys_addr(page_size) : 0ul;
        pt_elem pte = pt_elem::make(frame, huge);
        parent[idx] = pte.word; 
    }
    return mmu::phys_cast<u64>(pt_elem(parent[idx]).phys << 12);
}

inline void ensure_valid_pte(virt_addr l1, unsigned idx, bool with_frame){
    if(*(l1+idx) == 0){ // if the children is an empty page
        u64 frame = with_frame ? frames_alloc_phys_addr(4096) : 0ul;
        pt_elem pte = pt_elem::make(frame, false);
        *(l1+idx) = pte.word; 
    }
}

void allocate_pte_range(void* virt, u64 size, bool init, bool huge);
/*
 * if we ever need wrappers around allocate_pte_range
void allocate_pte_range_init(void* virt, u64 size){ allocate_pte_range(virt, size, true, false); }
void allocate_pte_range_huge(void* virt, u64 size){ allocate_pte_range(virt, size, false, true); }
void allocate_pte_range_init_huge(void* virt, u64 size) { allocate_pte_range(virt, size, true, true); }
*/

inline std::atomic<u64>* walkRef(void* virt, bool huge = false) {
    virt_addr ptRoot = mmu::phys_cast<u64>(processor::read_cr3());
    virt_addr l3 = mmu::phys_cast<u64>(pt_elem(ptRoot[idx(virt, 3)]).phys<<12);
    virt_addr l2 = mmu::phys_cast<u64>(pt_elem(l3[idx(virt, 2)]).phys<<12);
    if(huge)
        return reinterpret_cast<std::atomic<u64>*>(l2+idx(virt, 1));
    virt_addr l1 = mmu::phys_cast<u64>(pt_elem(l2[idx(virt, 1)]).phys<<12);
    return reinterpret_cast<std::atomic<u64>*>(l1+idx(virt, 0));
}

inline PTE walk(void* virt) {
   PTE pte = PTE(*walkRef(virt));
   return pte;
}

inline PTE walkHuge(void* virt){
    PTE pte = PTE(*walkRef(virt, true));
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

inline void invalidateTLBEntry(void* addr) {
   asm volatile("invlpg (%0)" ::"r" (addr) : "memory");
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

struct VMA;
struct Buffer{
   std::atomic<u64>* pteRefs[16]; 
   PTE snapshot[16];
   BufferState snapshotState;
   void* baseVirt;
   VMA* vma;
   int nb; // max 16 for 64KiB max size (+ 2MiB)
   // overhead (max for buffer size == sizeSmallPage) => 8+8+4+8+4 = 32B = 0.78%

   Buffer(void* addr, u64 size, VMA* vma);
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

inline void invlpg_tlb_all(std::vector<Buffer*> toFlush){
    return;
}

typedef u64 PID; // page id type

static const int16_t maxQueueSize = 512;
static const int16_t blockSize = 512;
static const u64 maxIOs = 4096;

inline void crash_osv(){
    printf("aborting\n");
    osv::halt();
}

// only allow 4KiB -> 64 KiB and 2MiB
inline int computeOrder(u64 size){
    switch(size){
        case 4096:
            return 0;
        case 8192:
            return 1;
        case 16384:
            return 2;
        case 32768:
            return 3;
        case 65536:
            return 4;
        case 20971252:
            return 9;
        default:
            return -1;
    }
}

inline bool isSupportedPageSize(u64 size){
    return computeOrder(size) != -1;
}

inline void* alignPage(void* addr, u64 pageSize){
    return reinterpret_cast<void*>(reinterpret_cast<u64>(addr) & ~(pageSize-1));
}

typedef std::vector<Buffer*>& prefetchList;
typedef std::vector<Buffer*>& evictList;

typedef float (*custom_score_func)(PID);
typedef u64 (*vm_to_storage_mapping_func)(VMA*, void*);
typedef bool (*isDirty_func)(Buffer* virt, int id);
typedef void (*setClean_func)(Buffer* virt, int id);
typedef void (*alloc_func)(VMA*, void*, prefetchList);
typedef void (*evict_func)(VMA*, u64, evictList);

class ResidentSet{
    public:
    struct Entry {
        std::atomic<u64> pid;
    };
    virtual bool insert(u64) = 0;
    virtual bool remove(u64) = 0;
    virtual u64 getNextBatch(u64 batchsize) = 0;
    virtual void getNextValidEntry(std::pair<u64, Entry*>*) = 0;
};

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
class HashTableResidentSet: public ResidentSet {
    public:
    static const u64 empty = ~0ull;
    static const u64 tombstone = (~0ull)-1;

    Entry* ht;
    u64 count;
    u64 mask;
    std::atomic<u64> clockPos;

    HashTableResidentSet(u64 maxCount);
    ~HashTableResidentSet();
    u64 next_pow2(u64 x);
    u64 hash(u64 k);
   
    bool insert(u64 pid) override;
    bool contains(u64 pid);
    bool remove(u64 pid) override;
    u64 getNextBatch(u64 batch) override;
    void getNextValidEntry(std::pair<u64, Entry*>* entry) override;
};

class VMA {
    public:
    void* start;
    u64 size;
    u64 lba_start;
    u64 pageSize;
    u64 id;
    std::atomic<u64> usedPhysSize;

    // policy related
    ResidentSet* residentSet;
    vm_to_storage_mapping_func vm_storage_map;
    isDirty_func isDirty_implem; 
    setClean_func setClean_implem;
    alloc_func prefetch_pol;
    evict_func evict_pol;
    

    VMA(u64 size, u64 page_size, u64 first_lba, isDirty_func dirty_func, setClean_func clean_func,
        ResidentSet* set, vm_to_storage_mapping_func f, alloc_func prefetch_policy, evict_func evict_policy);
    VMA(u64 size, u64 page_size, u64 first_lba);
    ~VMA(){}

    PID getPID(void* addr){
        assert(isValidPtr(addr));
        return ((u64)alignPage(addr, pageSize) - (u64)start)/pageSize;
    }

    void* getPtr(PID pid){
        return start + (pid * pageSize);
    }

    bool isValidPtr(void* addr){
        return addr >= start && addr < start + size;
    }

    u64 getStorageLocation(void* addr){
        return vm_storage_map(this, addr);
    }

    bool isDirty(Buffer* buf, int id){
        return isDirty_implem(buf, id);
    }

    void setClean(Buffer* buf, int id){
        return setClean_implem(buf, id);
    }

    void choosePrefetchingCandidates(void* addr, prefetchList pl){
        prefetch_pol(this, addr, pl);
    }

    void chooseEvictionCandidates(u64 sizeToEvict, evictList el){
        evict_pol(this, sizeToEvict, el);
    }
};

inline u64 linear_mapping(VMA* vma, void* addr){
    return vma->lba_start+(((u64)addr - (u64)vma->start)/blockSize);
}

inline bool pte_isDirty(Buffer* buf, int id){
    return buf->at(id).dirty == 1;
}

inline void pte_setClean(Buffer* buf, int id){
    buf->updateSnapshot();
    PTE oldPTE = buf->at(id);
    if(oldPTE.dirty == 0)
        return;
    PTE cleanPTE = PTE(oldPTE.word);
    cleanPTE.dirty = 0;
    buf->pteRefs[id]->compare_exchange_strong(oldPTE.word, cleanPTE.word); // if another thread wrote to it in the mean time just ignore
}

// by default -> no prefetching
inline void default_prefetch(VMA* vma, void* addr, prefetchList pl){
    return;
}

void default_transparent_eviction(VMA*, u64, evictList);

class uCache {	
    public:
	  // core
    std::map<u64, VMA*> vmas;
   	u64 totalPhysSize;
  
    // accessory
   	u64 batch;

   	// accounting
	  std::atomic<u64> usedPhysSize;
   	std::atomic<u64> readSize;
   	std::atomic<u64> writeSize;

   	uCache(u64 physSize, int batch);
   	~uCache();

    void* addVMA(u64 virtSize, u64 pageSize, u64 first_lba=0);
    VMA* getVMA(void* start);

   	const unvme_ns_t* ns;
	
	  // Functions

   	void ensureFreePages(u64 additionalSize);
   	void readBuffer(Buffer* buf);
    void readBufferToTmp(Buffer* buf, Buffer* tmp);
    void flush(std::vector<Buffer*> toWrite);
    
    void handleFault(u64 vmaID, void* addr, exception_frame *ef); // called from the page fault handler
   	void evict();

};

extern uCache* uCacheManager;

inline void createCache(u64 physSize, int batch){
    uCacheManager = new uCache(physSize, batch); 
}

}; // namespace ucache
#endif
