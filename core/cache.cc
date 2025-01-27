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
#include <smp.hh>

using namespace std;

TRACEPOINT(trace_cache_pf, "");
TRACEPOINT(trace_cache_pf_ret, "");
TRACEPOINT(trace_cache_evict, "");
TRACEPOINT(trace_cache_evict_ret, "");
TRACEPOINT(trace_cache_fixX, "");
TRACEPOINT(trace_cache_fixX_ret, "");
TRACEPOINT(trace_cache_fixS, "");
TRACEPOINT(trace_cache_fixS_ret, "");

// contains objects necessary to do IO operations
// a single structure is easier to do PERCPU/__thread depending
struct IOtoolkit {
    Page* tempPage;
    int id;
    unvme_iod_t io_descriptors[maxQueueSize];
    /*Log* write_logs;
    u64 wlindex;
    u64 size_log;*/

    IOtoolkit(int i){
        tempPage = (Page*) mmap(NULL, pageSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        memset(tempPage, 0, pageSize);
        madvise(tempPage, pageSize, MADV_DONTNEED);
        std::atomic<u64> *ptePtr = walkRef(tempPage);
        ptePtr->store(0ull);
        for(int i=0; i<maxQueueSize; i++){
            io_descriptors[i] = nullptr;
        }
        id = i;
        //size_log = 10000000;
        //write_logs = (Log*)malloc(sizeof(Log) * size_log);
        //wlindex = 0;
    };
};

PERCPU(IOtoolkit*, percpu_IOtoolkit);

inline IOtoolkit& get_IOtoolkit(){
    return **percpu_IOtoolkit;
}

void initIOtoolkits(){
    int i=0;
    for(auto c: sched::cpus){
        auto *ptp = percpu_IOtoolkit.for_cpu(c);
        *ptp = new IOtoolkit(i++);
    }
}

// allocate memory using huge pages
void* allocHuge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}

/*void print_backlog(CacheManager* cm, PID page){
    smp_crash_other_processors();
    for(sched::cpu* cpu: sched::cpus){
        bool printed = false;
        IOtoolkit *iotk = *(percpu_IOtoolkit.for_cpu(cpu));
        for(u64 i=0; i<iotk->size_log; i++){
            if((iotk->write_logs+i)->pid == page){
                if(!printed){
                    cout << "core: " << cpu->id << endl;
                    printed = true;
                }
                cout << (iotk->write_logs+i)->tsc << " ";
            }
        }
        if(printed)
            cout << endl;
    }
    u64 phys = walk(cm->toPtr(page)).phys;
    cout << "phys: " << phys << endl;
    assert(phys != 0);
    for(u64 i=0; i<cm->virtCount; i++){
        if(walk(cm->toPtr(i)).phys == phys){
            printf("The frame is mapped to multiple pages: %lu\n", i);
        }
    }
}*/

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

template<class Fn> void ResidentPageSet::iterateClockBatch(u64 batch, Fn fn, CacheManager* cm, PageLists *pl) {
      u64 pos, newPos;
      do {
         pos = clockPos.load();
         newPos = (pos+batch) % count;
      } while (!clockPos.compare_exchange_strong(pos, newPos));

      for (u64 i=0; i<batch; i++) {
         u64 curr = ht[pos].pid.load();
         if ((curr != tombstone) && (curr != empty))
            fn(cm, curr, pl);
         pos = (pos + 1) & mask;
      }
      /*if(pl->dirty != 0 || pl->rest != 0 || pl->failed_to_remove != 0){
        printf("clearA: %u, dirty: %u, rest: %u, failed to remove: %u\n", pl->clearA, pl->dirty, pl->rest, pl->failed_to_remove);
      }*/
   }

void ResidentPageSet::print(){
    for(u64 i=0; i<count; i++){
        if(ht[i].pid.load() != empty && ht[i].pid.load() != tombstone){
            printf("pid: %lu\n", ht[i].pid.load());
        }
    }
}

CacheManager::CacheManager(u64 virtSize, u64 physSize, int batch) : virtSize(virtSize), physSize(physSize), virtCount(virtSize / pageSize), physCount(physSize / pageSize), batch(batch) {
   	residentSet.init(physCount);
   	assert(virtSize>=physSize);
   	u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

   	pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
   	for (u64 i=0; i<virtCount; i++){
    	  pageState[i].init();
    }
    
   	ns = unvme_openq(sched::cpus.size(), maxQueueSize);
    initIOtoolkits();
    initYmaps();

   	// Initialize virtual pages
   	virtMem = (Page*) mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   	madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
   	if (virtMem == MAP_FAILED){
    	cerr << "mmap failed" << endl;
      assert(false);
    }

    PTE pte = PTE(0ull);
    pte.writable=1;
    for(u64 i=0; i<virtCount; i++){
	      memset(virtMem+i, 0, pageSize);	
		    madvise(virtMem+i, pageSize, MADV_DONTNEED);
		    // install zeroPages
		    atomic<u64>* ptePtr = walkRef(virtMem+i);
		    ptePtr->store(pte.word);
   	}
   	invalidateTLB();
    for(u64 i=0; i<virtCount; i++){
        assert(walk(virtMem+i).word == pte.word);
    }
   
   	physUsedCount = 0;
   	readCount = 0;
   	writeCount = 0;
    pfCount = 0;
    eviction_batch_func = default_transparent_eviction;

    cout << "MMIO Region initialized at "<< virtMem <<" with virtmem size : " << virtSize/gb << " GB, physmem size : " << physSize/gb << " GB" << endl;
}

CacheManager::~CacheManager(){};

void CacheManager::handleFault(PID pid, exception_frame *ef){
    trace_cache_pf();
    atomic<u64> *pteRef = walkRef(toPtr(pid));
    //printf("pf: %lu\n", pid);
    u64 phys;

    if(ef != NULL){
        page_fault_error_code ec = page_fault_error_code(ef->error_code);
        if(ec.write == 1 && PTE(*pteRef).present == 1){
            //cout << bitset<64>(PTE(*pteRef).word) << endl;
            printf("write access to unwritable page\n");
            crash_osv();
        }
    }

    if(PTE(*pteRef).present==1){
        if(PTE(*pteRef).phys == 0){
            printf("present 1 but phys 0. before softfault\n");
            assert(false);
        }
        goto exit;
    }
    if(PTE(*pteRef).present == 0){
        /*while(PTE(*pteRef).user == 1){
            _mm_pause();
        }*/
        if(PTE(*pteRef).user == 1){
            return; // TODO: should we spin here or simply return ?
        }
        if(PTE(*pteRef).phys != 0){ // soft page fault
            trySetPresent(toPtr(pid)); // if another thread already resolved the soft fault its good too
            goto exit;
        }
    }

    ensureFreePages();

    if(PTE(*pteRef).present==1){
        if(PTE(*pteRef).phys == 0){
            printf("present 1 but phys 0. after softfault\n");
            assert(false);
        }
        goto exit;
    }

    // get a physical frame
    phys =  ymap_getPage(0);
    assert(ymap_tryMap(get_IOtoolkit().tempPage, phys));
    readPageAt(pid, get_IOtoolkit().tempPage);
    if(ymap_tryMap(toPtr(pid), phys)){ //thread that won the race
        readCount++;
        pfCount++;
        physUsedCount++;
        assert(residentSet.insert(pid));
    }else{
        ymap_putPage(phys, 0); // put back unused candidates
    }
    ymap_unmap(get_IOtoolkit().tempPage);
    invalidateTLBEntry(get_IOtoolkit().tempPage);
exit:
    assert(walk(toPtr(pid)).user == 0);
    if(walk(toPtr(pid)).present == 1){
        assert(walk(toPtr(pid)).phys != 0);
    }
    //cout << "frame " << bitset<64>(walk(toPtr(pid)).phys) << endl;
    trace_cache_pf_ret();
}

void default_explicit_eviction(CacheManager* cm, PID pid, PageLists* pl) {
    PageState& ps = cm->getPageState(pid);
    u64 v = ps.stateAndVersion;
    switch (PageState::getState(v)) {
        case PageState::Marked:
            if (cm->virtMem[pid].dirty) {
                if (ps.tryLockS(v))
                    pl->toWrite->push_back(pid);
            } else {
                pl->toEvict->push_back(pid);
            }
            break;
        case PageState::Unlocked:
            ps.tryMark(v);
            break;
        default:
            break; // skip
    };
}

void default_transparent_eviction(CacheManager* cm, PID pid, PageLists* pl){
    PTE oldPTE = walk(cm->virtMem+pid);
    if(oldPTE.accessed == 0){ // if not accessed since it was cleared
        if(oldPTE.present == 0){
            if(cm->residentSet.remove(pid)){ // remove it from the RS to take "ownership" of the eviction
                if (oldPTE.dirty == 1){
                    if(trySetToWrite(cm->toPtr(pid), oldPTE.word)){ // if dirty soft unmap + set user
                        pl->toWrite->push_back(pid);
                        return;
                    }
                    assert(cm->residentSet.insert(pid));
                    return;
                }else{
                    pl->toEvict->push_back(pid);
                    return;
                }
            }else{
                return;
            // if the removal failed just return
            }
        }else{ // present == 1
            trySetNotPresent(cm->toPtr(pid), oldPTE.word);
        }
    }else{ // accessed == 1
        if(tryClearAccessed(cm->virtMem+pid, oldPTE.word)){
        }
        return;
        // does not matter if it fails
    }
}

void CacheManager::evict(){
    trace_cache_evict();
    vector<PID> toEvict;
    vector<void*> toEvictAddresses;
    vector<void*> toFlushAddresses;
    toEvict.reserve(batch);
    toEvictAddresses.reserve(batch);
    toFlushAddresses.reserve(batch);
    vector<PID> toWrite;
    toWrite.reserve(batch);

    PageLists pl;
    pl.toEvict = &toEvict;
    pl.toWrite = &toWrite;
    // 0. find candidates, lock dirty ones in shared mode
    while (toEvict.size()+toWrite.size() < batch) {
        residentSet.iterateClockBatch(batch, eviction_batch_func, this, &pl);
    }

    // pages in toEvict and toWrite are soft unmapped
    
    // checking if the page have been remapped only improve performance
    // we need to settle on which pages to flush from the TLB at some point anyway
    // since we need to batch TLB eviction
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
        if(walk(toPtr(pid)).present == 1){
            assert(residentSet.insert(pid)); // return the page to the RS
            return true;
        }else{
            toFlushAddresses.push_back(virtMem+pid);
            return false;
        }
    }), toEvict.end());
    toWrite.erase(std::remove_if(toWrite.begin(), toWrite.end(), [&](PID pid) {
        if(walk(toPtr(pid)).present == 1){
            assert(residentSet.insert(pid)); // return the page to the RS
            return true;
        }else{
            toFlushAddresses.push_back(virtMem+pid);
            return false;
        }
    }), toWrite.end()); 

    if(toFlushAddresses.size() < 33){
        mmu::invlpg_tlb_all(toEvictAddresses);
    }else{
        mmu::flush_tlb_all();
    }

    // now all accesses will trigger a page fault 
    
    assert(toWrite.size() <= maxQueueSize);
    // write dirty pages and add them to the toEvict list 
    flush(toWrite, &toEvict);
    
    u64 actuallyEvicted = 0;
    for(PID pid: toEvict){
        if(walk(toPtr(pid)).present == 0){ // no other thread remapped the page in the mean time
            u64 phys = ymap_tryUnmap(virtMem + pid);
            assert(phys != 0); // this evictor owns the eviction so no chance of concurrent eviction
            ymap_putPage(phys, 0); // send back the physical page to the pool
            // after this point the page has completely left the cache and any access will trigger 
            // a whole new allocation
            actuallyEvicted++;
        }else{
            assert(residentSet.insert(pid)); // return the page to the RS
        }
    }
    physUsedCount -= actuallyEvicted;
    /*// 2. try to lock clean page candidates
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
        PageState& ps = getPageState(pid);
        u64 v = ps.stateAndVersion;
        return (walk(virtMem+pid).accessed == 1 || !ps.tryLockX(v)); // remove if another accessed it since last time or if it was locked
    }), toEvict.end());


    // 3. try to upgrade lock for dirty page candidates
    for (auto& pid : toWrite) {
        PageState& ps = getPageState(pid);
        u64 v = ps.stateAndVersion;
        if(walk(virtMem+pid).accessed == 0 && PageState::getState(v) == 1 && ps.stateAndVersion.compare_exchange_weak(v, PageState::sameVersion(v, PageState::Locked)))
            toEvict.push_back(pid);
        else
            ps.unlockS(); 
    }*/
    
    /*std::vector<PID> markedNotPresent;
    for(u64& pid: toEvict){
        if(walk(virtMem+pid).accessed == 0){
            if(trySetNotPresent(virtMem+pid)){
                markedNotPresent.push_back(pid);
                toEvictAddresses.push_back(virtMem+pid);
            }else{ // couldn't soft unmap the page
                PageState& ps = getPageState(pid);
                ps.unlockX();
            }
        }else{ // page was accessed in the mean time
            PageState& ps = getPageState(pid);
            ps.unlockX();
        }
    }

    if(markedNotPresent.size() < 33){
        mmu::invlpg_tlb_all(toEvictAddresses);
    }else{
        mmu::flush_tlb_all();
    }
    
    for(auto& pid: toWrite){
        PageState& ps = getPageState(pid);
        ps.unlockS();
    }

    std::vector<PID> actuallyEvicted;
    // 4. remove from page table
    for (u64& pid : markedNotPresent){
        if(walk(virtMem+pid).accessed == 0 && walk(virtMem+pid).present == 0){ // no thread re-accessed since we flushed the TLB
            u64 phys = ymap_tryUnmap(virtMem + pid);
            if(phys){ // this thread got to remove the phys addr
                actuallyEvicted.push_back(pid);
                ymap_putPage(phys);
            }
        }else{ // page stays in the cache so unlock it
            PageState& ps = getPageState(pid);
            ps.unlockX();
        }
    }

    // 5. remove from hash table and unlock
    for (u64& pid : actuallyEvicted) {
        bool succ = residentSet.remove(pid);
        assert(succ);
        getPageState(pid).unlockXEvicted();
        clearUser(toPtr(pid));
    }

    physUsedCount -= actuallyEvicted.size();*/
    trace_cache_evict_ret();
}

void CacheManager::ensureFreePages() {
    if (physUsedCount >= physCount*0.95)
        evict();
}

void CacheManager::readPage(PID pid) {
    readPageAt(pid, toPtr(pid));
    readCount++;
}

void CacheManager::readPageAt(PID pid, void* virt) {
    int ret = unvme_read(ns, get_IOtoolkit().id, virt, pid*(pageSize/blockSize), pageSize/blockSize);
    assert(ret==0);
}

void CacheManager::fix(PID pid){
    physUsedCount++;
    ensureFreePages();
    u64 phys = ymap_getPage(0);
    assert(ymap_tryMap(toPtr(pid), phys));
    readPage(pid);
    pfCount++;
    residentSet.insert(pid);
}

void CacheManager::flush(std::vector<PID> toWrite, std::vector<PID>* toEvict){
    int iod_used=0;
    std::vector<PID> copy;
    toWrite.erase(std::remove_if(toWrite.begin(), toWrite.end(), [&](PID pid){
        copy.push_back(pid);
        if(trySetClean(reinterpret_cast<void*>(virtMem+pid))){
            get_IOtoolkit().io_descriptors[iod_used] = unvme_awrite(ns, get_IOtoolkit().id, toPtr(pid), pid*(pageSize/blockSize), pageSize/blockSize);
            assert(get_IOtoolkit().io_descriptors[iod_used]);
            iod_used++;
            toEvict->push_back(pid);
            return false;
        }else{
            // abort writing
            clearUser(toPtr(pid));
            assert(residentSet.insert(pid)); // return the page to the RS
            return true;
        }
    }), toWrite.end());
    for(int i=0; i<iod_used; i++){
        int ret=unvme_apoll(get_IOtoolkit().io_descriptors[i], 3);
        if(ret!=0){
            std::cout << "IO error, writing with ret " << ret << ", queue: " << get_IOtoolkit().id << ", i " << i << ", pid " << toWrite.at(i) << std::endl;
        }
        assert(ret==0);
        clearUser(toPtr(toWrite.at(i)));
    }
    for(PID pid: copy){
        if(walk(toPtr(pid)).user == 1){
            printf("Should not happen\n");
            crash_osv();
        }
    }
    writeCount += iod_used;
}

std::vector<CacheManager*> mmio_regions;

CacheManager* createMMIORegion(void* start, u64 virtSize, u64 physSize, int batch){
    assert(virtSize % pageSize == 0);
    CacheManager* cache = new CacheManager(virtSize, physSize, batch);
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
    free(cache->virtMem);
    delete cache;
}
