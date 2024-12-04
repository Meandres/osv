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
#include <osv/trace.hh>

using namespace std;

__thread int workerThreadId __attribute__ ((tls_model ("initial-exec"))) = -1;
__thread measure_t local_measure __attribute__ ((tls_model ("initial-exec"))) = {0};
measure_t global_measure = {0};
std::mutex measure_mutex;

TRACEPOINT(trace_cache_pf, "");
TRACEPOINT(trace_cache_pf_ret, "");
TRACEPOINT(trace_cache_evict, "");
TRACEPOINT(trace_cache_evict_ret, "");
TRACEPOINT(trace_cache_fixX, "");
TRACEPOINT(trace_cache_fixX_ret, "");
TRACEPOINT(trace_cache_fixS, "");
TRACEPOINT(trace_cache_fixS_ret, "");

std::atomic<u64> evictCount(0);
std::atomic<u64> pfCount(0);

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

bool ResidentPageSet::insert(u64 pid) {
    u64 pos = hash(pid) & mask;
    while (true) {
        u64 curr = ht[pos].pid.load();
        if(curr == pid){
            return false;
        }
        assert(curr != pid);
        if ((curr == empty) || (curr == tombstone))
            if (ht[pos].pid.compare_exchange_strong(curr, pid))
                return true;
        pos = (pos + 1) & mask;
    }
}

bool ResidentPageSet::contains(u64 pid) {
    u64 pos = hash(pid) & mask;
    while (true) {
        u64 curr = ht[pos].pid.load();
        if(curr == pid){
            return true;
        }
        assert(curr != pid);
        if ((curr == empty) || (curr == tombstone))
            return false;
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

template<class Fn>
void execute_in_parallel(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
   std::vector<std::thread> threads;
   uint64_t n = end-begin;
   if (n<nthreads)
      nthreads = n;
   uint64_t perThread = n/nthreads;
   for (unsigned i=0; i<nthreads; i++) {
      threads.emplace_back([&,i]() {
         uint64_t b = (perThread*i) + begin;
         uint64_t e = (i==(nthreads-1)) ? end : ((b+perThread) + begin);
         fn(i, b, e);
      });
   }
   for (auto& t : threads)
      t.join();
}

CacheManager::CacheManager(u64 virtSize, u64 physSize, int n_threads, int batch,  bool ex_cont = false) : explicit_control(ex_cont), virtSize(virtSize), physSize(physSize), n_threads(n_threads), nb_queues_used(n_threads < maxQueues ? n_threads : maxQueues), virtCount(virtSize / pageSize), physCount(physSize / pageSize), batch(batch), ymapBundle(physCount, n_threads) {
   	assert(n_threads<=maxWorkerThreads);
   	
   	residentSet.init(physCount);
   	assert(virtSize>=physSize);
   	u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

   	pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
    pagePin = (PagePin*)allocHuge(virtCount * sizeof(PagePin));
   	for (u64 i=0; i<virtCount; i++){
    	pageState[i].init();
        pagePin[i].init();
    }

   	emptyPages = (Page*) mmap(NULL, n_threads*pageSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    madvise(emptyPages, n_threads*pageSize, MADV_NOHUGEPAGE);
    for(int i=0; i<n_threads; i++){
        memset(emptyPages+i, 0, pageSize);
        madvise(emptyPages+i, pageSize, MADV_DONTNEED);
		// install zeroPages
		atomic<u64>* ptePtr = walkRef(emptyPages+i);
		ptePtr->store(0ull);
    }

   	ns = unvme_openq(nb_queues_used, maxQueueSize);
	io_descriptors.reserve(nb_queues_used*ns->qsize);
	for(int i=0; i<n_threads; i++){
        for(int j=0; j<(int)ns->qsize; j++){
            io_descriptors.push_back(NULL);
        }
		freeIDList.push_back(i);
	}

   	// Initialize virtual pages
   	virtMem = (Page*) mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   	madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
    lastPage = virtMem + virtSize;
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

    cout << "MMIO Region initialized at "<< virtMem <<" with virtmem size : " << virtSize/gb << " GB, physmem size : " << physSize/gb << " GB, max threads : " << n_threads;
    if(explicit_control)
        cout << ", explicit control";
    else
        cout << ", page fault control";
    cout <<endl;
}

CacheManager::~CacheManager(){};

void CacheManager::handleFault(PID pid){
    trace_cache_pf();
    handlers[CACHE_OP_PAGEFAULT](this, pid, -1);
    trace_cache_pf_ret();
}

int CacheManager::allocate(PID* listStart, int size){
    return handlers[CACHE_OP_ALLOC](this, (u64)listStart, size);
}

void CacheManager::evict(){
    trace_cache_evict();
    handlers[CACHE_OP_EVICT](this, 0, 0);
    trace_cache_evict_ret();
}

void CacheManager::ensureFreePages() {
    //if(tid<nb_queues_used){
        if (physUsedCount >= physCount*0.95)
            evict();
    /*}else{
        while(physUsedCount >= physCount*0.95){}
    }*/
}

// allocated new page and fix it
Page* CacheManager::allocPage() {
    physUsedCount++;
    ensureFreePages();
    u64 pid = allocCount++;
    if (pid >= virtCount) {
        cerr << "VIRTGB is too low" << endl;
        exit(EXIT_FAILURE);
    }
    u64 stateAndVersion = getPageState(pid).stateAndVersion;
    bool succ = getPageState(pid).tryLockX(stateAndVersion);
    assert(succ);
    int tid = getTID();
    if(tid == -1){
        ymapBundle.mapPhysPage(0, toPtr(pid));
    }else{
       ymapBundle.mapPhysPage(tid, toPtr(pid));
    }
    residentSet.insert(pid);
    return toPtr(pid);
}

Page* CacheManager::fixX(PID pid) {
    trace_cache_fixX();
    PageState& ps = getPageState(pid);
    for (u64 repeatCounter=0; ; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion.load();
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)) {
                    explicitFix(pid);
                    trace_cache_fixX_ret();
                    return virtMem + pid;
                }
                break;
            }
            case PageState::Marked: case PageState::Unlocked: {
                if (ps.tryLockX(stateAndVersion)){
                    trace_cache_fixX_ret();
                    return virtMem + pid;
                }
                break;
            }
        }
        yield(repeatCounter);
    }
}

Page* CacheManager::fixS(PID pid) {
    trace_cache_fixS();
    PageState& ps = getPageState(pid);
    for (u64 repeatCounter=0; ; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion;
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Locked: {
                break;
            } case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)){
                    explicitFix(pid);
                    ps.unlockX();
                }
                break;
            } default: {
                if (ps.tryLockS(stateAndVersion)){
                    trace_cache_fixS_ret();
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
    int ret = unvme_read(ns, getTID()%maxQueues, toPtr(pid), pid*(pageSize/blockSize), pageSize/blockSize);
    assert(ret==0);
    readCount++;
}

void CacheManager::readPageAt(PID pid, void* virt) {
    int ret = unvme_read(ns, getTID()%maxQueues, virt, pid*(pageSize/blockSize), pageSize/blockSize);
    assert(ret==0);
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

void CacheManager::explicitFix(PID pid){
    physUsedCount++;
    ensureFreePages();
    if(getTID() == -1){
        ymapBundle.mapPhysPage(0, toPtr(pid));
    }else{
        ymapBundle.mapPhysPage(getTID(), toPtr(pid));
    }
    if(get_stored_bit(toPtr(pid))){ 
        // reuse "user mode" bit of the PTE to indicate if the page has already been resolved in the past
        // this is due to TPCC allocating pages "before they are backed by storage"
        // 0 -> never resolved before, 1 -> resolved then evicted
        readPage(pid);
    }
    residentSet.insert(pid);
}

void CacheManager::sync(std::vector<PID> pages, int tid){
    for (u64 i=0; i<pages.size(); i++) {
        PID pid = pages[i];
        // clear dirty bit
        std::atomic<u64>* ptePtr = walkRef(toPtr(pid));
        PTE pte = PTE(ptePtr->load());
        pte.dirty = 0;
        ptePtr->store(pte.word);
        io_descriptors[tid*ns->qsize + i] = unvme_awrite(ns, tid, toPtr(pid), pid*(pageSize/blockSize), pageSize/blockSize);
        assert(io_descriptors[tid*ns->qsize + i]);
    }
    for(u64 i=0; i<pages.size(); i++){
        int ret=unvme_apoll(io_descriptors[tid*ns->qsize + i], 3);
        if(ret!=0){
            std::cout << "Error ret " << ret << ", queue: " << tid << ", i " << i << ", pid " << pages[i] << std::endl;
        }
        assert(ret==0);
    }
}

int default_handleFault(CacheManager* cm, PID pid, int unused) {
    pfCount++;
    PagePin& pp = cm->getPagePin(pid);
    pp.add();
    PageState& ps = cm->getPageState(pid);
    u64 stateAndVersion = ps.stateAndVersion.load();
    if(!ps.tryValidate(stateAndVersion)){
        while(!cm->residentSet.contains(pid)){_mm_pause();}
        return 0;
    }
    cm->ensureFreePages();
    u64 phys;
    int tid = cm->getTID() == -1 ? 0 : cm->getTID();
    phys = cm->ymapBundle.getPage(tid);
    assert(cm->ymapBundle.tryMap(tid, cm->emptyPages+tid, phys));
    if(get_stored_bit(cm->toPtr(pid))){ 
        // reuse "user mode" bit of the PTE to indicate if the page has already been resolved in the past
        // this is due to TPCC allocating pages "before they are backed by storage"
        // 0 -> never resolved before, 1 -> resolved then evicted
        cm->readPageAt(pid, cm->emptyPages+tid);
    }
    if(cm->ymapBundle.tryMap(tid, cm->toPtr(pid), phys)){ // thread that emplaced the page
        cm->physUsedCount++;
        cm->readCount++;
        cm->residentSet.insert(pid);
    }else{
        cm->ymapBundle.ymapInterfaces[tid]->lock();
        cm->ymapBundle.putPage(tid, phys);
        cm->ymapBundle.ymapInterfaces[tid]->unlock();
    }
    pp.decr();
    cm->ymapBundle.ymapInterfaces[tid]->clearPhysAddr(cm->emptyPages+tid);
    invalidateTLBEntry(cm->emptyPages+tid);
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
    assert(tid<cm->nb_queues_used);

    // 0. find candidates, lock dirty ones in shared mode
    while (toEvict.size()+toWrite.size() < cm->batch) {
        cm->residentSet.iterateClockBatch(cm->batch, [&](PID pid) {
            PagePin& pp = cm->getPagePin(pid);
            if(pp.hasWaiters())
                return;
            PageState& ps = cm->getPageState(pid);
            u64 v = ps.stateAndVersion;
            switch (PageState::getState(v)) {
                case PageState::Marked:
                    //if (cm->virtMem[pid].dirty) {
                    if (walk(cm->toPtr(pid)).dirty == 1) {
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
        assert(toWrite.size() <= maxQueueSize);
        for (u64 i=0; i<toWrite.size(); i++) {
            PID pid = toWrite[i];
            // clear dirty bit
            std::atomic<u64>* ptePtr = walkRef(cm->toPtr(pid));
            PTE pte = PTE(ptePtr->load());
            pte.dirty = 0;
            ptePtr->store(pte.word);
            cm->io_descriptors[tid*cm->ns->qsize + i] = unvme_awrite(cm->ns, tid, cm->toPtr(pid), pid*(pageSize/blockSize), pageSize/blockSize);
            assert(cm->io_descriptors[tid*cm->ns->qsize + i]);
        }
        for(u64 i=0; i<toWrite.size(); i++){
            int ret=unvme_apoll(cm->io_descriptors[tid*cm->ns->qsize + i], 3);
            if(ret!=0){
                std::cout << "Error ret " << ret << ", queue: " << tid << ", i " << i << ", pid " << toWrite[i] << std::endl;
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

        std::vector<PID> actuallyEvicted;
        // 4. remove from page table
        for (u64& pid : toEvict){
            PagePin& pp = cm->getPagePin(pid);
            if(!pp.hasWaiters()){
                cm->ymapBundle.unmapPhysPage(tid, cm->virtMem + pid);
                actuallyEvicted.push_back(pid);
                toEvictAddresses.push_back(cm->virtMem+pid);
            }
        }
        mmu::invlpg_tlb_all(toEvictAddresses);

        // 5. remove from hash table and unlock
        for (u64& pid : actuallyEvicted) {
            bool succ = cm->residentSet.remove(pid);
            assert(succ);
            cm->getPageState(pid).unlockXEvicted();
        }

        cm->physUsedCount -= actuallyEvicted.size();
        return 0;
    }

    std::vector<CacheManager*> mmio_regions;

    CacheManager* createMMIORegion(void* start, u64 virtSize, u64 physSize, int nb_threads, int batch, bool ex_cont){
        assert(virtSize % pageSize == 0);
        CacheManager* cache = new CacheManager(virtSize, physSize, nb_threads, batch, ex_cont);
        mmio_regions.push_back(cache);
        return cache;
    }

    void destroyMMIORegion(CacheManager* cache){
        int i=0;
        for(CacheManager* mmr: mmio_regions){
            if(mmr == cache)
                break;
            i++;
        }
        mmio_regions.erase(mmio_regions.begin()+i);
        free(cache->pageState);
        execute_in_parallel(0, cache->virtCount, cache->n_threads, [&](uint64_t threads, uint64_t begin, uint64_t end){
                cache->registerThread();
                for(u64 i=begin; i<end; i++){
                atomic<u64>* ptePtr = walkRef(cache->virtMem+i);
                ptePtr->store(0ull);
                }
                cache->forgetThread();
                });
        free(cache->virtMem);
        delete cache;
    }
