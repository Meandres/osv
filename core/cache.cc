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

#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

#include <osv/mmu.hh>
#include <osv/cache.hh>

using namespace std;

std::atomic<u64> pageFaultNumber(0);
std::atomic<u64> evictCount(0);
std::mutex thread_mutex;
__thread elapsed_time parts_time[parts_num] = { };
__thread uint64_t parts_count[parts_num] = { };
elapsed_time thread_aggregate_time[parts_num] = { };
uint64_t thread_aggregate_count[parts_num] = { };

// allocate memory using huge pages
void* allocHuge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}

// use when lock is not free
void yield(u64 counter) {
   _mm_pause();
}
ResidentPageSet::ResidentPageSet(){}

void ResidentPageSet::init(u64 maxCount){
	//count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
	count = next_pow2(maxCount * 1.5);
	mask = count-1;
	clockPos = 0;
	ht = (Entry*)allocHuge(count * sizeof(Entry));
	memset((void*)ht, 0xFF, count * sizeof(Entry));
}

ResidentPageSet::~ResidentPageSet() {
    munmap(ht, count * sizeof(u64));
}

u64 ResidentPageSet::next_pow2(u64 x) {
	return 1<<(64-__builtin_clzl(x-1));
}

u64 ResidentPageSet::hash(u64 k) {
    const u64 m = 0xc6a4a7935bd1e995;
    const int r = 47;
    u64 h = 0x8445d61a4e774912 ^ (8*m);
    k *= m;
    k ^= k >> r;
    k *= m;
	h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

void ResidentPageSet::insert(u64 pid) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid.load();
         assert(curr != pid);
         if ((curr == empty) || (curr == tombstone))
            if (ht[pos].pid.compare_exchange_strong(curr, pid))
               return;

         pos = (pos + 1) & mask;
      }
   }

bool ResidentPageSet::remove(u64 pid) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid.load();
         if (curr == empty)
            return false;

         if (curr == pid)
            if (ht[pos].pid.compare_exchange_strong(curr, tombstone))
               return true;

         pos = (pos + 1) & mask;
      }
   }

template<class Fn> void ResidentPageSet::iterateClockBatch(u64 batch, Fn fn) {
      u64 pos, newPos;
      do {
         pos = clockPos.load();
         newPos = (pos+batch) % count;
      } while (!clockPos.compare_exchange_strong(pos, newPos));

      for (u64 i=0; i<batch; i++) {
         u64 curr = ht[pos].pid.load();
         if ((curr != tombstone) && (curr != empty))
            fn(curr);
         pos = (pos + 1) & mask;
      }
   }
CacheManager::CacheManager(u64 virtSize, u64 physSize, int n_threads, int batch,  bool ex_cont = false) : explicit_control(ex_cont), virtSize(virtSize), physSize(physSize), n_threads(n_threads), virtCount(virtSize / pageSize), physCount(physSize / pageSize), batch(batch), ymapBundle(physCount, n_threads) {
   	assert(n_threads<=maxWorkerThreads);
	if(n_threads > maxQueues){
		std::cout << "Beware you are using more threads than nvme queues. This might go horribly wrong !" << std::endl;
	}
   	
   	residentSet.init(physCount);
   	assert(virtSize>=physSize);
   	u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

   	pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
   	for (u64 i=0; i<virtCount; i++)
    	pageState[i].init();

   	ns = unvme_open();
	io_descriptors.reserve(n_threads*ns->qsize);
	for(int i=0; i<n_threads; i++){
        for(int j=0; j<(int)ns->qsize; j++){
            io_descriptors.push_back(NULL);
        }
		freeIDList.push_back(i);
	}

   	// Initialize virtual pages
   	virtMem = (Page*) mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   	madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
   	if (virtMem == MAP_FAILED)
    	cerr << "mmap failed" << endl;
    for(u64 i=0; i<virtCount; i++){
		memset(virtMem+i, 0, pageSize);
	
		madvise(virtMem+i, pageSize, MADV_DONTNEED);
		// install zeroPages
		atomic<u64>* ptePtr = walkRef(virtMem+i);
		ptePtr->store(0ull);
   	}
   	invalidateTLB();
    for(u64 i=0; i<virtCount; i++){
        assert(walk(virtMem+i).word == 0ull);
    }
   
   	physUsedCount = 0;
   	allocCount = 1; // pid 0 reserved for meta data
   	readCount = 0;
   	writeCount = 0;

    handlers.reserve(nb_cache_op);
    handlers.push_back(&default_handleFault);
    handlers.push_back(NULL);
    handlers.push_back(&default_allocate);
    handlers.push_back(&default_evict);

    cout << "MMIO Region initialized at "<< virtMem <<" with virtmem size :" << virtSize/gb << " GB, physmem size : " << physSize/gb << " GB, max threads : " << n_threads << endl;
}

CacheManager::~CacheManager(){};

void CacheManager::handleFault(PID pid){
    auto start = getClock();
    handlers[CACHE_OP_PAGEFAULT](this, pid, 0);
    addTime(start, handlefault);
}

int CacheManager::allocate(PID* listStart, int size){
    return handlers[CACHE_OP_ALLOC](this, (u64)listStart, size);
}

void CacheManager::evict(){
    auto start = getClock();
    handlers[CACHE_OP_EVICT](this, 0, 0);
    addTime(start, evictpage);
}

void CacheManager::ensureFreePages() {
   if (physUsedCount >= physCount*0.95)
      evict();
}

// allocated new page and fix it
Page* CacheManager::allocPage() {
    auto start = osv::clock::uptime::now();
    u64 pid = allocCount++;
    if (pid >= virtCount) {
        cerr << "VIRTGB is too low" << endl;
        exit(EXIT_FAILURE);
    }
    u64 stateAndVersion = getPageState(pid).stateAndVersion;
    bool succ = getPageState(pid).tryLockX(stateAndVersion);
    assert(succ);
    if(explicit_control){ 
        physUsedCount++;
        ensureFreePages();
        int tid = getTID();
        if(tid == -1){
            parts_time[mapphys] += ymapBundle.mapPhysPage(0, virtMem+pid);
            parts_count[mapphys]++;
        }else{
            parts_time[mapphys] += ymapBundle.mapPhysPage(tid, virtMem+pid);
            parts_count[mapphys]++;
        }
        residentSet.insert(pid);
    }
    addTime(start, allocpage);
    return toPtr(pid);
}

Page* CacheManager::fixX(PID pid) {
    auto start = osv::clock::uptime::now();
    PageState& ps = getPageState(pid);
    for (u64 repeatCounter=0; ; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion.load();
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)) {
                    if(explicit_control)
                        handleFault(pid);
                    addTime(start, fixx);
                    return virtMem + pid;
                }
                break;
            }
            case PageState::Marked: case PageState::Unlocked: {
                if (ps.tryLockX(stateAndVersion)){
                    addTime(start, fixx);
                    return virtMem + pid;
                }
                break;
            }
        }
        yield(repeatCounter);
    }
}
/*
Page* CacheManager::fixS(PID pid) {
    auto start = osv::clock::uptime::now();
    PageState& ps = getPageState(pid);
    for (u64 repeatCounter=0; ; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion;
        if(explicit_control){
            switch (PageState::getState(stateAndVersion)) {
                case PageState::Locked: {
                    break;
                } case PageState::Evicted: {
                    if (ps.tryLockX(stateAndVersion)){
                        handleFault(pid);
                        ps.unlockX();
                    }
                    break;
                } default: {
                    if (ps.tryLockS(stateAndVersion)){
                        addTime(start, fixs);
                        return virtMem + pid;
                    }
                }
            }
        }else{
            switch (PageState::getState(stateAndVersion)) {
                case PageState::Locked: {
                    break;
                } default: {
                    if (ps.tryLockS(stateAndVersion)){
                        addTime(start, fixs);
                        return virtMem + pid;
                    }
                }
            }
        }
        yield(repeatCounter);
    }
}*/

Page* CacheManager::fixS(PID pid) {
    auto start = osv::clock::uptime::now();
    PageState& ps = getPageState(pid);
    for (u64 repeatCounter=0; ; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion;
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Locked: {
                break;
            } case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)){
                    if(explicit_control)
                        handleFault(pid);
                    ps.unlockX();
                }
                break;
            } default: {
                if (ps.tryLockS(stateAndVersion)){
                    addTime(start, fixs);
                    return virtMem + pid;
                }
            }
        }
        yield(repeatCounter);
    }
}

void CacheManager::unfixS(PID pid) {
   getPageState(pid).unlockS();
}

void CacheManager::unfixX(PID pid) {
   getPageState(pid).unlockX();
}

void CacheManager::readPage(PID pid) {
    auto start = osv::clock::uptime::now();
    int ret = unvme_read(ns, getTID()%maxQueues, virtMem+pid, pid*(pageSize/blockSize), pageSize/blockSize);
    assert(ret==0);
    readCount++;
    addTime(start, readpage);
}

int CacheManager::updateCallback(enum cache_opcode op, cache_op_func_t f){
    if(op>nb_cache_op)
        return 1;
    if(op < nb_cache_op)
        handlers[op] = f;
    else
        handlers.push_back(f);
    return 0;
}

int nb_cache_op = 4;

int default_handleFault(CacheManager* cm, PID pid, int unused) {
    PageState& ps = cm->getPageState(pid);
    u64 stateAndVersion = ps.stateAndVersion.load();
    if(PageState::getState(stateAndVersion) == PageState::Evicted){
        ps.tryValidate(stateAndVersion);
    }
    cm->physUsedCount++;
    cm->ensureFreePages();
    if(cm->getTID() == -1){
        parts_time[mapphys] += cm->ymapBundle.mapPhysPage(0, cm->virtMem+pid);
        parts_count[mapphys]++;
    }else{
        parts_time[mapphys] += cm->ymapBundle.mapPhysPage(cm->getTID(), cm->virtMem+pid);
        parts_count[mapphys]++;
    }
    cm->readPage(pid);
    cm->residentSet.insert(pid);
    return 0;
}

int default_allocate(CacheManager* cm, u64 listStart, int size){
    return 0;
}

int default_evict(CacheManager* cm, u64 unused_u64, int unused_int) {
    vector<PID> toEvict;
    vector<void*> toEvictAddresses;
    toEvict.reserve(cm->batch);
    toEvictAddresses.reserve(cm->batch);
    vector<PID> toWrite;
    toWrite.reserve(cm->batch);
    int tid = cm->getTID();
    
    // 0. find candidates, lock dirty ones in shared mode
    while (toEvict.size()+toWrite.size() < cm->batch) {
        cm->residentSet.iterateClockBatch(cm->batch, [&](PID pid) {
            PageState& ps = cm->getPageState(pid);
            u64 v = ps.stateAndVersion;
            switch (PageState::getState(v)) {
                case PageState::Marked:
                    //if (cm->virtMem[pid].dirty) {
                    if (walk(cm->virtMem+pid).dirty == 1) {
                        if (ps.tryLockS(v))
                            toWrite.push_back(pid);
                    } else {
                        toEvict.push_back(pid);
                    }
                    break;
                case PageState::Unlocked:
                    ps.tryMark(v);
                    break;
                default:
                    break; // skip
            };
        });
    }

    // 1. write dirty pages
    assert(toWrite.size() <= maxIOs);
    for (u64 i=0; i<toWrite.size(); i++) {
	    PID pid = toWrite[i];
        // clear dirty bit
        std::atomic<u64>* ptePtr = walkRef(cm->virtMem+pid);
        PTE pte = PTE(ptePtr->load());
        pte.dirty = 0;
        ptePtr->store(pte.word);
        cm->io_descriptors[tid*cm->ns->qsize + i] = unvme_awrite(cm->ns, tid%maxQueues, (cm->virtMem)+pid, pid*(pageSize/blockSize), pageSize/blockSize);
	    assert(cm->io_descriptors[tid*cm->ns->qsize + i]);
    }
    for(u64 i=0; i<toWrite.size(); i++){
	    int ret=unvme_apoll(cm->io_descriptors[tid*cm->ns->qsize + i], 3);
	    if(ret!=0){
		    std::cout << "Error ret " << ret << ", i " << i << ", pid " << toWrite[i] << std::endl;
	    }
	    assert(ret==0);
    }
    cm->writeCount += toWrite.size();
    evictCount += toWrite.size() + toEvict.size();
   
   // 2. try to lock clean page candidates
   toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
      PageState& ps = cm->getPageState(pid);
      u64 v = ps.stateAndVersion;
      return (PageState::getState(v) != PageState::Marked) || !ps.tryLockX(v);
   }), toEvict.end());
   

   // 3. try to upgrade lock for dirty page candidates
   for (auto& pid : toWrite) {
      PageState& ps = cm->getPageState(pid);
      u64 v = ps.stateAndVersion;
      if ((PageState::getState(v) == 1) && ps.stateAndVersion.compare_exchange_weak(v, PageState::sameVersion(v, PageState::Locked)))
         toEvict.push_back(pid);
      else
         ps.unlockS();
   }
   
   // 4. remove from page table
    for (u64& pid : toEvict){
	    parts_time[unmapphys] += cm->ymapBundle.unmapPhysPage(tid, cm->virtMem + pid);
        parts_count[unmapphys] ++;
        toEvictAddresses.push_back(cm->virtMem+pid);
    }
    mmu::invlpg_tlb_all(toEvictAddresses);
   
   // 5. remove from hash table and unlock
    for (u64& pid : toEvict) {
        bool succ = cm->residentSet.remove(pid);
        assert(succ);
        cm->getPageState(pid).unlockXEvicted();
    }

   cm->physUsedCount -= toEvict.size();
   return 0;
}

std::vector<CacheManager*> mmio_regions;

CacheManager* createMMIORegion(void* start, u64 virtSize, u64 physSize, int nb_threads, int batch, bool ex_cont){
	assert(virtSize % pageSize == 0);
    CacheManager* cache = new CacheManager(virtSize, physSize, nb_threads, batch, ex_cont);
	mmio_regions.push_back(cache);
    return cache;
}
