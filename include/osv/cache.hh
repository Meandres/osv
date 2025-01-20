#ifndef CACHE_HH
#define CACHE_HH

#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
#include <span>

#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <osv/types.h>
#include <unistd.h>
#include <immintrin.h>

#include <cstring>
#include <drivers/nvme.hh>
#include <osv/ymap.hh>
#include <osv/mmu.hh>
#include <osv/power.hh>

#include <chrono>
#include <mutex>

#ifndef VMCACHE

typedef u64 PID; // page id type

const u64 mb = 1024ull * 1024;
const u64 gb = 1024ull * 1024 * 1024;

static const int16_t maxWorkerThreads = 128;
static const int16_t maxQueues = 128;
static const int16_t maxQueueSize = 256;
static const int16_t blockSize = 512;
static const u64 maxIOs = 4096;

struct Log {
    u64 tsc;
    PID pid;

    Log(PID p){
        tsc = rdtsc();
        pid = p;
    }
};

// allocate memory using huge pages
void* allocHuge(size_t size);

inline void crash_osv(){
    printf("aborting\n");
    osv::halt();
}

// use when lock is not free
void yield(u64 counter);

struct PageState {
    std::atomic<u64> stateAndVersion;

    static const u64 Unlocked = 0;
    static const u64 MaxShared = 251;
    static const u64 Locked = 253;
    static const u64 Marked = 254;
    static const u64 Evicted = 255;

    PageState() {}

    void init() { stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release); }
    static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion<<8)>>8) | newState<<56; }
    static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion<<8)>>8)+1) | newState<<56; }

    bool tryLockX(u64 oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
    }

    void unlockX() {
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
    }

    void unlockXEvicted() {
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
    }
    
    void unlockXSameVersion() {
        assert(getState() == Locked);
        stateAndVersion.store(sameVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
    }

    void downgradeLock() {
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), 1), std::memory_order_release);
    }

    bool tryLockS(u64 oldStateAndVersion) {
        u64 s = getState(oldStateAndVersion);
        if (s<MaxShared)
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s+1));
        if (s==Marked)
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
        return false;
    }

    void unlockS() {
        while (true) {
            u64 oldStateAndVersion = stateAndVersion.load();
            u64 state = getState(oldStateAndVersion);
            assert(state>0 && state<=MaxShared);
            if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state-1)))
                return;
        }
    }

    bool tryMark(u64 oldStateAndVersion) {
        assert(getState(oldStateAndVersion)==Unlocked);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
    }

    bool tryValidate(u64 oldStateAndVersion){
        if(getState(oldStateAndVersion)!=Evicted)
            return false;
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Unlocked));
    }

    void setToUnlockedIfMarked(){
        assert(getState() == 0 || getState() >= 254);
        if(getState() == Evicted)
            return;
        stateAndVersion.store(sameVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
    }

    static u64 getState(u64 v) { return v >> 56; };
    u64 getState() { return getState(stateAndVersion.load()); }

    void operator=(PageState&) = delete;
};

class CacheManager;
typedef float (*custom_score_func)(PID);
struct PageLists{
    std::vector<PID>* toEvict;
    std::vector<PID>* toWrite;
};
typedef void (*custom_batch_func)(CacheManager*, PID, PageLists*);
typedef std::vector<PID>* candidate_list;

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
struct ResidentPageSet {
    static const u64 empty = ~0ull;
    static const u64 tombstone = (~0ull)-1;

    struct Entry {
        std::atomic<u64> pid;
    };

    Entry* ht;
    u64 count;
    u64 mask;
    std::atomic<u64> clockPos;

    ResidentPageSet();
    void init(u64 maxCount);
    ~ResidentPageSet();
    u64 next_pow2(u64 x);
    u64 hash(u64 k);
   
    bool insert(u64 pid);
    bool contains(u64 pid);
    bool remove(u64 pid);
    void print();
    template<class Fn> void iterateClockBatch(u64 batch, Fn fn, CacheManager* cm, PageLists* pl);
};

class CacheManager {	
    public:
	  // core
   	Page* virtMem;
	  u64 virtSize;
   	u64 physSize;
  
	  // accessory
	  u64 virtCount;
   	u64 physCount;
   	u64 batch;

   	// accounting
	  std::atomic<u64> physUsedCount;
   	std::atomic<u64> readCount;
   	std::atomic<u64> writeCount;
    std::atomic<u64> pfCount;

	  // interface <-> application
   	PageState* pageState;
   	ResidentPageSet residentSet;

   	CacheManager(u64 virtSize, u64 physSize, int batch);
   	~CacheManager();

   	const unvme_ns_t* ns;
	
	  // Functions
   	PageState& getPageState(PID pid) {
    	  return pageState[pid];
   	}

   	bool isValidPtr(uintptr_t page) { 
        uintptr_t start = reinterpret_cast<uintptr_t>(virtMem);
        return (page >= start) && (page < (start + virtSize + 16)); 
    }
   	PID toPID(void* page) { 
        if(!isValidPtr(reinterpret_cast<uintptr_t>(page))){
            crash_osv();
        }
        return reinterpret_cast<Page*>(page) - virtMem; 
    }
   	Page* toPtr(PID pid) { 
        if(pid < 0 || pid >= virtCount){
            crash_osv();
        }
        return virtMem + pid; 
    }

    void setMemoryToUnwritable(){
        for(u64 i = 0; i<virtCount; i++){
            std::atomic<u64> *pteRef = walkRef(toPtr(i));
            PTE pte = PTE(*pteRef);
            pte.writable = 0;
            pteRef->store(pte.word);
        }
        mmu::flush_tlb_all();
    }

   	void ensureFreePages();
   	void readPage(PID pid);
    void readPageAt(PID pid, void* virt);
    
    void flush(std::vector<PID> toWrite, std::vector<PID>* toEvict);
    void handleFault(PID pid, exception_frame *ef); // called from the page fault handler
    void fix(PID pid); // explicit call
   	void evict();

    candidate_list alloc_candidates;
    candidate_list evict_candidates;
    custom_score_func allocation_score_func;
    custom_score_func eviction_score_func;
    custom_batch_func allocation_batch_func;
    custom_batch_func eviction_batch_func;

};

void default_explicit_eviction(CacheManager*, PID, PageLists*);
void default_transparent_eviction(CacheManager*, PID, PageLists*);

extern std::vector<CacheManager*> mmio_regions;

inline CacheManager* get_mmr(uintptr_t addr){
	for(CacheManager* mmr: mmio_regions){
		if(mmr->isValidPtr(addr)){
			return mmr;
		}
	}
	return NULL;
}

void print_backlog(CacheManager *cm, PID page);
inline void cache_handle_page_fault(CacheManager* mmr, uintptr_t addr, exception_frame *ef){
	  mmr->handleFault(mmr->toPID((void*)addr), ef);
}

// TODO: replace with File when we implement it
// for now we just create one region
// int createMMAPRegion(void* start, size_t size, void* file, size_t size);
CacheManager* createMMIORegion(void* start, u64 virtSize, u64 physSize, int batch);
void destroyMMIORegion(CacheManager* cache);

struct OLCRestartException{};

#endif
#endif
