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
#include <fstream>
#include <sys/mman.h>
#include "drivers/poll_nvme.hh"

#include <osv/types.h>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <osv/percpu.hh>
#include <osv/llfree.h>
#include <osv/power.hh>

static const u64 maxPageSize = mmu::huge_page_size;
#ifndef THRESHOLD_INVLPG_FLUSH
#define THRESHOLD_INVLPG_FLUSH 1024
#endif

namespace ucache {

extern int next_available_file_id;
struct ucache_file{
    int id;
    std::string name;
    u64 start_lba;
    u64 num_lb;
    u64 size;
    
    u64 lba_size;
    u64 current_seek_pos;

    ucache_file(std::string n, u64 slba, u64 size, u64 blksize): id(next_available_file_id++), name(n), start_lba(slba), num_lb(ceil(size/(blksize+0.0))), size(size), lba_size(blksize), current_seek_pos(0)
    {
    }
};

extern u64 next_available_slba;
extern std::vector<ucache_file*> available_nvme_files;

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
    static u64 valid_mask(){
        pt_elem pte;
        pte.word = ~(0ull);
        pte.unused = 0;
        pte.unused2 = 0;
        pte.no_execute = 0;
        pte.huge_page_null = 0;
        return ~(pte.word);
    }
};

typedef pt_elem PTE;

static unsigned idx(void *virt, unsigned level)
{
    return ((u64)virt >> (12 + level * 9)) & 511;
}

inline virt_addr ensure_valid_pt_elem(virt_addr parent, unsigned idx, bool with_frame, bool huge=false){
    if(pt_elem(parent[idx]).phys == 0 || pt_elem(parent[idx]).present == 0 || pt_elem(parent[idx]).writable == 0 || (parent[idx] & pt_elem::valid_mask()) != 0){ // if the children is an empty page
        u64 page_size = huge ? 2ul*1024*1024 : 4096;
        u64 frame = with_frame ? frames_alloc_phys_addr(page_size) : 0ul;
        pt_elem pte = pt_elem::make(frame, huge);
        parent[idx] = pte.word;
        if(with_frame) // initialize new page
            memset(mmu::phys_cast<u64>(pte.phys << 12), 0, 4096);
    }
    return mmu::phys_cast<u64>(pt_elem(parent[idx]).phys << 12);
}

inline void initialize_pte(virt_addr l1, unsigned idx, bool with_frame){
    u64 frame = with_frame ? frames_alloc_phys_addr(4096) : 0ul;
    pt_elem pte = pt_elem::make(frame, false);
    *(l1+idx) = pte.word; 
}
inline void ensure_valid_pte_debug(virt_addr l1, unsigned idx, bool with_frame){
    if(*(l1+idx) == 0){ // if the children is an empty page
        u64 frame = with_frame ? frames_alloc_phys_addr(4096) : 0ul;
        pt_elem pte = pt_elem::make(frame, false);
        std::cout << std::bitset<64>(pte.word) << std::endl;
        *(l1+idx) = pte.word; 
        std::cout << std::bitset<64>(*(l1+idx)) << std::endl;
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
   Resolving,
   Prefetching, // currently being prefetched
   Writing, // currently being written
   Evicting, // chosen for eviction
   Unmapped, // phys == 0 and present == 0
   Inconsistent, // fail
   TBD // something to work with
};

struct VMA;
struct Buffer {
   std::vector<std::atomic<u64>*> pteRefs; // change this to dynamically 
   std::vector<PTE> snapshot;
   std::atomic<BufferState> snapshotState;
   void* baseVirt;
   std::atomic<u16> dirty;
   VMA* vma;

   Buffer(void* addr, u64 size, VMA* vma);
   ~Buffer(){}

  void updateSnapshot(){
    BufferState oldState = snapshotState.load();
    BufferState newState = BufferState::TBD;
    for(size_t i = 0; i < pteRefs.size(); i++){
        snapshot[i] = PTE(pteRefs[i]->load());
        if(oldState == BufferState::Evicting || oldState == BufferState::Prefetching || oldState == BufferState::Writing || oldState == BufferState::Resolving){
            // do not update the state if it being processed
            // the relevant functions should change the state directly
            return;
        }
        if(newState == BufferState::Inconsistent){
            break; // skip since we already reached an inconsistent state
        }
        if(snapshot[i].phys != 0){
            if(snapshot[i].present == 1){ // phys != 0 && present == 1
                if(newState == BufferState::Mapped || newState == BufferState::TBD)
                    newState = BufferState::Mapped;
                else
                    newState = BufferState::Inconsistent; 
            }else{ // phys != 0 && present == 0
                newState = BufferState::Inconsistent;
            }
        }else{
            if(snapshot[i].present == 0){ // phys == 0 && present == 0
                if(newState == BufferState::Unmapped || newState == BufferState::TBD)
                    newState = BufferState::Unmapped;
                else
                    newState = BufferState::Inconsistent;
            }else // phys == 0 && present == 1
                newState = BufferState::Inconsistent;
        }
    }
    snapshotState.compare_exchange_strong(oldState, newState);
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

typedef std::vector<Buffer*>& PrefetchList;
typedef std::vector<Buffer*>& EvictList;

typedef float (*custom_score_func)(Buffer*);
typedef bool (*isDirty_func)(Buffer* virt);
typedef void (*setClean_func)(Buffer* virt);
typedef void (*alloc_func)(VMA*, void*, PrefetchList, void*);
typedef void (*evict_func)(VMA*, u64, EvictList);

class ResidentSet{
    public:
    struct Entry {
        std::atomic<Buffer*> buf;
    };
    virtual bool insert(Buffer*) = 0;
    virtual bool remove(Buffer*) = 0;
    virtual u64 getNextBatch(u64 batchsize) = 0;
    virtual void getNextValidEntry(std::pair<u64, Entry*>*) = 0;
};

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
class HashTableResidentSet: public ResidentSet {
    public:
    static const uintptr_t empty = ~0ull;
    static const uintptr_t tombstone = (~0ull)-1;

    Entry* ht;
    u64 count;
    u64 mask;
    std::atomic<u64> clockPos;

    HashTableResidentSet(u64 maxCount);
    ~HashTableResidentSet();
    u64 next_pow2(u64 x);
    u64 hash(u64 k);
   
    bool insert(Buffer* buf) override;
    bool contains(Buffer* buf);
    bool remove(Buffer* buf) override;
    u64 getNextBatch(u64 batch) override;
    void getNextValidEntry(std::pair<u64, Entry*>* entry) override;
};

class VMA {
    public:
    void* start;
    u64 size;
    struct ucache_file* file;
    u64 pageSize;
    u64 id;
    std::atomic<u64> usedPhysSize;
    Buffer* buffers;

    // policy related
    ResidentSet* residentSet;
    isDirty_func isDirty_implem; 
    setClean_func setClean_implem;
    alloc_func prefetch_pol;
    evict_func evict_pol;

    void* prefetchObject;

    VMA(u64 size, u64 page_size, isDirty_func dirty_func, setClean_func clean_func,
        ResidentSet* set, struct ucache_file*, alloc_func prefetch_policy, evict_func evict_policy);
    static VMA* newVMA(u64 size, u64 page_size);
    static VMA* newVMA(const char* name, u64 page_size);
    ~VMA(){}

    Buffer* getBuffer(void* addr){
        if(!isValidPtr(addr)){
            return NULL;
        }
        u64 index = ((u64)alignPage(addr, pageSize) - (u64)start)/pageSize;
        return buffers+index;
    }

    bool isValidPtr(void* addr){
        uintptr_t s = (uintptr_t)start;
        uintptr_t a = (uintptr_t)addr;
        return a >= s && a < s + size;
    }

    u64 getStorageLocation(void* addr){
        return file->start_lba + ((uintptr_t)addr-(uintptr_t)start)/file->lba_size;
    } 

    bool isDirty(Buffer* buf){
        return isDirty_implem(buf);
    }

    void setClean(Buffer* buf){
        return setClean_implem(buf);
    }

    void choosePrefetchingCandidates(void* addr, PrefetchList pl){
        prefetch_pol(this, addr, pl, prefetchObject);
    }

    void chooseEvictionCandidates(u64 sizeToEvict, EvictList el){
        evict_pol(this, sizeToEvict, el);
    }

    bool addEvictionCandidate(Buffer* buf, EvictList el){
        BufferState oldState = buf->snapshotState.load();
        if(residentSet->remove(buf)){
            if(buf->snapshotState.compare_exchange_strong(oldState, BufferState::Evicting)){
                el.push_back(buf);
                return true;
            }else{
                assert(residentSet->insert(buf));
            }
        }
        return false;
    }
};

inline bool pte_isDirty(Buffer* buf){
	int dirty = 0;
	for(size_t i=0; i<buf->pteRefs.size(); i++){
		dirty += buf->snapshot[i].dirty;
	}
	return dirty > 0;
}

inline void pte_setClean(Buffer* buf){
  for(size_t i = 0; i < buf->pteRefs.size(); i++){ // just try to 
		PTE pte = buf->snapshot[i];
		if(pte.dirty == 0){ // simply skip
			continue;
		}
		PTE newPTE = PTE(pte.word);
		newPTE.dirty = 0; 
		buf->pteRefs[i]->compare_exchange_strong(pte.word, newPTE.word); // best effort 
	}
}

// by default -> no prefetching
inline void default_prefetch(VMA* vma, void* addr, PrefetchList pl, void* obj){
    return;
}

void default_transparent_eviction(VMA*, u64, EvictList);

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
    std::atomic<u64> tlbFlush;

   	uCache(u64 physSize, int batch);
   	~uCache();

    void* addVMA(u64 virtSize, u64 pageSize);
    VMA* getOrCreateVMA(const char* name);
    VMA* getVMA(void* start);
    Buffer* getBuffer(void* addr);

    const unvme_ns_t* ns;
	  
    // Functions
   	void ensureFreePages(u64 additionalSize);
   	void readBuffer(Buffer* buf);
    void readBufferToTmp(Buffer* buf, Buffer* tmp);
    void flush(std::vector<Buffer*> toWrite);
    
    void handleFault(u64 vmaID, void* addr, exception_frame *ef); // called from the page fault handler
    void prefetch(VMA* vma, PrefetchList pl);
   	void evict();

};

extern uCache* uCacheManager;

void createCache(u64 physSize, int batch);

inline void discover_ucache_files(){
    std::ifstream files("/nvme_files.txt");
    std::string name;
    u64 slba, size;
    std::cout << "Available files on the ssd:" << std::endl;
    while(files >> name >> slba >> size){
        ucache_file* file = new ucache_file(name, slba, size, uCacheManager->ns->blocksize);
        std::cout << name << ": from " << slba << ", size: " << size << std::endl;
        available_nvme_files.push_back(file);
        next_available_slba += file->num_lb + 1;
    }

}

inline struct ucache_file* find_ucache_file(const char* name){
    for(struct ucache_file* file: available_nvme_files){
        if(strcmp(file->name.c_str(), name) == 0){
            //printf("Found file: %s from %lu with %lu blocks (of size %u)\n", file->name.c_str(), file->start_lba, file->num_lb, uCacheManager->ns->blocksize);
            return file;
        }
    }
    //printf("Error: could not find %s\n", name);
    return NULL;
}
}; // namespace ucache
#endif
