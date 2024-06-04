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
#include <drivers/ymap.hh>
#include <osv/mmu.hh>

#include <chrono>
#include <mutex>

#ifndef VMCACHE
typedef u64 PID; // page id type
extern std::atomic<u64> evictCount;

const u64 mb = 1024ull * 1024;
const u64 gb = 1024ull * 1024 * 1024;

static const int16_t maxWorkerThreads = 128;
static const int16_t maxQueues = 128;
static const int16_t maxQueueSize = 256;
static const int16_t blockSize = 512;
static const u64 maxIOs = 4096;

// allocate memory using huge pages
void* allocHuge(size_t size);

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
       assert(getState(oldStateAndVersion)==Evicted);
       return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Unlocked));
   }

   static u64 getState(u64 v) { return v >> 56; };
   u64 getState() { return getState(stateAndVersion.load()); }

   void operator=(PageState&) = delete;
};

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
   bool remove(u64 pid);
   template<class Fn> void iterateClockBatch(u64 batch, Fn fn);
};
class CacheManager;

enum cache_opcode {
    CACHE_OP_PAGEFAULT = 0, // argument : u64 addr -> no return value
    CACHE_OP_IOERROR = 1, //
    CACHE_OP_ALLOC = 2, // argument : list of u64 addr, at min addr of the array + size -> no return value (?)
    CACHE_OP_EVICT = 3,
};
extern int nb_cache_op;
typedef int (*cache_op_func_t)(CacheManager*, u64, int);

class CacheManager {	
    public:
    const bool explicit_control;
	// core
   	Page* virtMem;
	u64 virtSize;
   	u64 physSize;
   	int n_threads;
    int nb_queues_used;
  
	// accessory
	u64 virtCount;
   	u64 physCount;
   	u64 batch;
    std::vector<unvme_iod_t> io_descriptors; // for n_threads * ns->qsize
	std::unordered_map<int, int> threadMap;
    std::vector<int> freeIDList;
    YmapBundle ymapBundle;
    std::atomic_flag lockFreeIDList = ATOMIC_FLAG_INIT;

   	// accounting
	std::atomic<u64> physUsedCount;
   	std::atomic<u64> allocCount;
   	std::atomic<u64> readCount;
   	std::atomic<u64> writeCount;

	// interface <-> application
   	PageState* pageState;
   	ResidentPageSet residentSet;

    // callbacks customization
    std::vector<cache_op_func_t> handlers;
    int updateCallback(enum cache_opcode, cache_op_func_t f);

   	CacheManager(u64 virtSize, u64 physSize, int n_threads, int batch, bool ex_cont);
   	~CacheManager();

   	const unvme_ns_t* ns;
	
	// Functions
   	PageState& getPageState(PID pid) {
    	return pageState[pid];
   	}

   	Page* fixX(PID pid);
   	void unfixX(PID pid);
   	Page* fixS(PID pid);
   	void unfixS(PID pid);

   	bool isValidPtr(void* page) { return (page >= virtMem) && (page < (virtMem + virtSize + 16)); }
   	PID toPID(void* page) { return reinterpret_cast<Page*>(page) - virtMem; }
   	Page* toPtr(PID pid) { return virtMem + pid; }

   	void ensureFreePages();
   	void readPage(PID pid);
   	Page* allocPage();
    
    int allocate(PID* listStart, int size);
   	void handleFault(PID pid);
   	void evict();

    inline void registerThread(){
        while(lockFreeIDList.test_and_set(std::memory_order_acquire)){
            _mm_pause();
        }
        assert(freeIDList.size()>=0);
		int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        if(freeIDList.empty()){
            printf("Overcommitment of threads. Failing\n");
            assert(false);
        }
        int id = freeIDList.back();
        threadMap.insert({tid, id}); 
        freeIDList.pop_back();
        //std::cout << "Mapped thread " << tid << " to ID " << id << std::endl;
        lockFreeIDList.clear(std::memory_order_release);
    }
    inline void forgetThread(){
		int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        while(lockFreeIDList.test_and_set(std::memory_order_acquire)){
            _mm_pause();
        }
        auto search = threadMap.find(tid);
        if (search != threadMap.end()){
			freeIDList.push_back(search->second);
            threadMap.erase(tid);
            //std::cout << "Unmapped thread " << tid << std::endl;
		}
        lockFreeIDList.clear(std::memory_order_release);
    }
	inline int getTID(){
		int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        auto search = threadMap.find(tid);
        if (search != threadMap.end())
            return search->second;
        return -1;
	}
};

// default op handlers
int default_allocate(CacheManager* cm, u64 listStart, int size);
int default_handleFault(CacheManager* cm, u64 pid, int);
int default_evict(CacheManager* cm, u64, int);

extern std::vector<CacheManager*> mmio_regions;

inline CacheManager* get_mmr(void* addr){
	for(CacheManager* mmr: mmio_regions){
		if(mmr->isValidPtr(addr)){
            assert(mmr->explicit_control!=true);
			return mmr;
		}
	}
	return NULL;
}

inline void cache_handle_page_fault(CacheManager* mmr, void* addr){
	mmr->handleFault(mmr->toPID(addr));
}

// TODO: replace with File when we implement it
// for now we just create one region
// int createMMAPRegion(void* start, size_t size, void* file, size_t size);
CacheManager* createMMIORegion(void* start, u64 virtSize, u64 physSize, int nb_threads, int batch, bool ex_cont);
void destroyMMIORegion(CacheManager* cache);
extern __thread int tls_in_kernel;
void increment_local_kernel_tls();

/*
// Base-level primitives
// Most basic building blocks of the cache (use IO/mm subsystems internally ofc)
// Those functions are NOT customizable and should be everything the custom handlers need

// create a virtual page of the given size (4KiB, 2MiB or 1GiB) in a region account
int alloc(void* vma, size_t size);
// remove a virtual page from a region account
int free(void* vma); 
// populate the page table on the range [address, address + size].
// Does not map the range to physical pages.
int populatePT(void* vma, size_t size); 

int map(void* vma);
int unmap(void* vma);
int map(void* vma, u64 pma);

int read(void* vma, File f, size_t offset);
int write(void* vma, File f, size_t offset);

// 1st level handlers

// Lock in Exclusive mode
int lockExclusive(void* vma);
// Lock in Shared mode.
int lockShared(void* vma);
int tryLockExclusive(void* vma);
int tryLockShared(void* vma);
// Unlock the page, either decrement the ref count if in shared lock or to Unlocked state if exclusive lock
int unlock(void* vma);

int fixExclusive(void* vma);
int fixShared(void* vma);
int evict(void* vma);

int read(void* vma);
int write(void* vma);
int alloc(void* vma);
int free(void* vma);

// 2nd level handlers
void handleFault(void* vma);
void evict();
void ioError();
*/
//struct OLCRestartException{};

//extern CacheManager cache;
#endif
#endif
