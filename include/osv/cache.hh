#ifndef CACHE_HH
#define CACHE_HH

#include <atomic>
#include <algorithm>
#include <cassert>
#include <fcntl.h>
#include <iostream>

#include <sys/mman.h>
#include <osv/types.h>
#include <unistd.h>
#include <immintrin.h>
#include <cmath>

#include <cstring>
#include <drivers/nvme.hh>
#include <osv/ymap.hh>
#include <osv/mmu.hh>
#include <osv/power.hh>

typedef u64 PID; // page id type

static const int16_t maxWorkerThreads = 128;
static const int16_t maxQueues = 128;
static const int16_t maxQueueSize = 512;
static const int16_t blockSize = 512;
static const u64 maxIOs = 4096;

inline void* zeroInitVM(u64 size, size_t alignment=sizeSmallPage){
    // this way we make sure that the vma is always aligned on the logical page size
    void* mr = mmu::map_anon_aligned(size, alignment);
    madvise(mr, size, MADV_NOHUGEPAGE);
    PTE emptyPte = PTE(0ull);
    emptyPte.writable = 1;
    u64 sizePage = alignment;
    u64 nb_pages = size/sizePage;
    for(u64 i = 0; i<nb_pages; i++){
        memset(mr+(i*sizePage), 0, sizePage);
        madvise(mr+(i*sizePage), sizePage, MADV_DONTNEED);
        std::atomic<u64>* ptePtr = walkRef(mr+(i*sizePage));
        ptePtr->store(emptyPte.word);
    }
    invalidateTLB();
    return mr;
}

// allocate memory using huge pages
void* allocHuge(size_t size);

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

struct VMA{
    void* start;
    u64 size;
    u64 pageSize;

    VMA(u64 size, u64 page_size): size(size) {
        assert(isSupportedPageSize(page_size));
        pageSize = page_size;
        start = zeroInitVM(size);
    }
    ~VMA();

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


    u64 getStorageLocation(void* addr){ // for now just place the only vma at the beginning of the lba 
        return ((u64)addr - (u64)start)/blockSize;
    }

    u64 getStorageLocation(PID pid){ // for now just place the only vma at the beginning of the lba 
        return pid*(pageSize/blockSize);
    }
};

// for now only one VMA, TODO: replace with actual tree/whatever we choose logic later on
struct VMATree{
    VMA* vma;

    VMATree(){
        vma = NULL;
    }
    ~VMATree();

    void addVMA(VMA* new_vma){
        vma = new_vma;
    }

    VMA* getVMA(void* addr){
        if(vma == NULL)
            return NULL;
        if(vma->isValidPtr(addr))
            return vma;
        return NULL;
    }

    bool isValidPtr(uintptr_t addr){
        return vma->isValidPtr(reinterpret_cast<void*>(addr)); // TODO: this is not used atm
    }
};

class CacheManager;
typedef float (*custom_score_func)(PID);
struct PageLists{
    std::vector<PID>* toEvict;
    std::vector<void*>* toWrite;
};
typedef void (*custom_batch_func)(PID, PageLists*);
typedef std::vector<PID>* candidate_list;

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
struct ResidentSet {
    static const u64 empty = ~0ull;
    static const u64 tombstone = (~0ull)-1;

    struct Entry {
        std::atomic<u64> pid;
    };

    Entry* ht;
    u64 count;
    u64 mask;
    std::atomic<u64> clockPos;

    ResidentSet();
    void init(u64 maxCount);
    ~ResidentSet();
    u64 next_pow2(u64 x);
    u64 hash(u64 k);
   
    bool insert(u64 pid);
    bool contains(u64 pid);
    bool remove(u64 pid);
    void print();
    template<class Fn> void iterateClockBatch(u64 batch, Fn fn, PageLists* pl);
};

class uCache {	
    public:
	  // core
    VMATree* vmaTree;
   	u64 totalPhysSize;
  
	  // accessory
   	u64 batch;

   	// accounting
	  std::atomic<u64> usedPhysSize;
   	std::atomic<u64> readSize;
   	std::atomic<u64> writeSize;
    std::atomic<u64> pfCount;

	  // interface <-> application
   	ResidentSet residentSet;

   	uCache(u64 physSize, int batch);
   	~uCache();

    void addVMA(u64 virtSize, u64 pageSize);

   	const unvme_ns_t* ns;
	
	  // Functions

   	bool isValidPtr(uintptr_t page) { 
        return vmaTree->isValidPtr(page); 
    }
   	PID toPID(void* addr) {
        VMA* vma = vmaTree->getVMA(addr);
        return vma->getPID(addr); 
    }
   	void* toPtr(PID pid) { 
        VMA* vma = vmaTree->vma; // TODO: this only works with a single VMA
        return vma->getPtr(pid);
    }

   	void ensureFreePages(u64 additionalSize);
   	void readPage(PID pid);
   	void readPage(void* addr, u64 size);
    void readPageAt(void* addr, void* virt, u64 size);
    
    void flush(std::vector<void*> toWrite, std::vector<PID>* aborted);
    void flush(std::vector<PID> toWrite);
    bool handleFault(void* addr, exception_frame *ef); // called from the page fault handler
    void fix(PID pid); // explicit call
   	void evict();

    candidate_list alloc_candidates;
    candidate_list evict_candidates;
    custom_score_func allocation_score_func;
    custom_score_func eviction_score_func;
    custom_batch_func allocation_batch_func;
    custom_batch_func eviction_batch_func;
};

void default_transparent_eviction(PID, PageLists*);

extern uCache* uCacheManager;

inline void createCache(u64 physSize, int batch){
    uCacheManager = new uCache(physSize, batch); 
}

#endif
