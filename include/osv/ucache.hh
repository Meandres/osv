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
            u64 io : 1; // currently being used by the IO driver
            u64 inserting : 1; // being inserted
            u64 evicting : 1; // being evicted
            u64 phys : 40; // physical address the page aligned 52bit physical address of the frame or the next page table
            u64 prefetcher : 11; // core id that initiated the prefetching
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
        pte.io = 0;
        pte.inserting = 0;
        pte.evicting = 0;
        pte.prefetcher = 0;
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

inline void invalidateTLBEntry(void* addr) {
   asm volatile("invlpg (%0)" ::"r" (addr) : "memory");
}

enum BufferState{
    Cached, // phys != 0 and present == 1
    Inserting,
    Reading, // currently being read
    ReadyToInsert, 
    Writing, // currently being written
    Evicting, // chosen for eviction
    Uncached, // phys == 0 and present == 0
    Inconsistent, // fail
    TBD // just something to work with
};

static BufferState computePTEState(PTE pte){
    if(pte.inserting == 1 && pte.evicting == 0 && pte.io == 0){
        return BufferState::Inserting;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 1 && pte.present == 0 && pte.phys != 0){
        return BufferState::Reading;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 0 && pte.phys != 0){
        return BufferState::ReadyToInsert;
    }
    if(pte.inserting == 0 && pte.evicting == 1 && pte.io == 0){
        return BufferState::Evicting;
    }
    if(pte.inserting == 0 && pte.evicting == 1 && pte.io == 1){
        return BufferState::Writing;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 1 && pte.phys != 0){
        return BufferState::Cached;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 0 && pte.phys == 0){
        return BufferState::Uncached;
    }
    return BufferState::Inconsistent;
}

struct VMA;

struct Buffer {
    std::vector<std::atomic<u64>*> pteRefs; 
    std::vector<PTE> ptes;
    void* baseVirt;
    VMA* vma;
    BufferState state; 

    Buffer(void* addr, u64 size, VMA* vma);
    ~Buffer(){}

    void updateSnapshot(){
        state = BufferState::TBD;
        for(size_t i = 0; i < pteRefs.size(); i++){
            ptes[i] = PTE(pteRefs[i]->load());
            BufferState w = computePTEState(ptes[i]);
            if(state == BufferState::TBD){
                state = w;
            }else{
                if(w != state && state != BufferState::Inserting && state != BufferState::Reading && state != BufferState::ReadyToInsert && state != BufferState::Evicting && state != BufferState::Writing){
                    state = BufferState::Inconsistent;
                    return;
                }
            }
        }
    }

    u64 getPrefetcher(){
        PTE pte(*pteRefs[0]);
        return pte.prefetcher;
    }

   void tryClearAccessed();

   bool tryMap(u64 phys);
   u64 tryUnmap();

   int getAccessed();

   // for non-concurrent scenarios
   void map(u64 phys);
   u64 unmap();
   void invalidateTLBEntries();

   // Transitions
   bool toInserting();
   bool toPrefetching(u64 phys);
   bool toEvicting();
   bool toCached();
   bool ReadyToCached();

   bool setIO();
   bool clearIO();
};

static const int16_t maxQueueSize = 256;
static const int16_t blockSize = 512;
static const u64 maxIOs = 4096;

inline void crash_osv(){
    printf("aborting\n");
    osv::halt();
}

inline void assert_crash(bool cond){
    if(!cond)
        crash_osv();
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
typedef bool (*isAccessed_func)(Buffer* virt);
typedef bool (*isDirty_func)(Buffer* virt);
typedef void (*clearDirty_func)(Buffer* virt);
typedef void (*setDirty_func)(Buffer* virt);
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

    // policy related
    ResidentSet* residentSet;
    isDirty_func isDirty_implem; 
    clearDirty_func clearDirty_implem;
    setDirty_func setDirty_implem;
    isAccessed_func isAccessed_implem;
    alloc_func prefetch_pol;
    evict_func evict_pol;

    void* prefetchObject;

    VMA(u64, u64, isDirty_func, clearDirty_func, setDirty_func, isAccessed_func,
        ResidentSet*, struct ucache_file*, alloc_func, evict_func);
    static VMA* newVMA(u64 size, u64 page_size);
    static VMA* newVMA(const char* name, u64 page_size);
    ~VMA(){}

    bool isValidPtr(void* addr){
        uintptr_t s = (uintptr_t)start;
        uintptr_t a = (uintptr_t)addr;
        return a >= s && a < s + size;
    }

    u64 getStorageLocation(void* addr){
        return file->start_lba + ((uintptr_t)addr-(uintptr_t)start)/file->lba_size;
    } 

    bool isAccessed(Buffer* buf){
        return isAccessed_implem(buf);
    }
    
    bool isDirty(Buffer* buf){
        return isDirty_implem(buf);
    }

    void clearDirty(Buffer* buf){
        return clearDirty_implem(buf);
    }

    void setDirty(Buffer* buf){
        return setDirty_implem(buf);
    }

    void choosePrefetchingCandidates(void* addr, PrefetchList pl){
        prefetch_pol(this, addr, pl, prefetchObject);
    }

    void chooseEvictionCandidates(u64 sizeToEvict, EvictList el){
        evict_pol(this, sizeToEvict, el);
    }

    bool addEvictionCandidate(Buffer* buf, EvictList el){
        //buf->updateSnapshot();
        if(residentSet->remove(buf)){
            if(buf->toEvicting()){
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
		dirty += buf->ptes[i].dirty;
	}
	return dirty > 0;
}

inline bool pte_isAccessed(Buffer* buf){
    int accessed = 0;
	  for(size_t i=0; i<buf->pteRefs.size(); i++){
		    accessed += buf->ptes[i].accessed;
	  }
	  return accessed > 0;
}

inline void pte_clearDirty(Buffer* buf){
  for(size_t i = 0; i < buf->pteRefs.size(); i++){ // just try to 
		PTE pte = buf->ptes[i];
		if(pte.dirty == 0){ // simply skip
			continue;
		}
		PTE newPTE = PTE(pte.word);
		newPTE.dirty = 0; 
		buf->pteRefs[i]->compare_exchange_strong(pte.word, newPTE.word); // best effort 
	}
}

inline void pte_setDirty(Buffer* buf){
    return;
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
    std::atomic<u64> pageFaults;
    std::atomic<u64> tlbFlush;

   	uCache(u64 physSize, int batch);
   	~uCache();

    void* addVMA(u64 virtSize, u64 pageSize);
    VMA* getOrCreateVMA(const char* name);
    VMA* getVMA(void* addr);

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
    std::string initial_text = "Available files on the ssd:\n";
    bool printed_initial=false;
    while(files >> name >> slba >> size){
        if(!printed_initial)
            std::cout << initial_text;
        ucache_file* file = new ucache_file(name, slba, size, uCacheManager->ns->blocksize);
        std::cout << name << ": from " << slba << ", size: " << size << std::endl;
        available_nvme_files.push_back(file);
        next_available_slba += file->num_lb + 1;
    }
    if(!printed_initial){
        printf("No files available on the SSD\n");
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
