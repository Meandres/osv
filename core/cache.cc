#include <atomic>
#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <vector>

#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

#include <osv/mmu.hh>
#include <osv/cache.hh>
#include <smp.hh>

using namespace std;

uCache* uCacheManager;

// contains objects necessary to do IO operations
// a single structure is easier to do PERCPU/__thread depending
struct IOtoolkit {
    void* tempPage;
    int id;
    unvme_iod_t io_descriptors[maxQueueSize];

    IOtoolkit(int i){
        tempPage = zeroInitVM(sizeHugePage);
        for(int i=0; i<maxQueueSize; i++){
            io_descriptors[i] = nullptr;
        }
        id = i;
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

ResidentSet::ResidentSet(){}

void ResidentSet::init(u64 maxCount){
	count = next_pow2(maxCount * 1.5);
	mask = count-1;
	clockPos = 0;
	ht = (Entry*)allocHuge(count * sizeof(Entry));
	memset((void*)ht, 0xFF, count * sizeof(Entry));
}

ResidentSet::~ResidentSet() {
    munmap(ht, count * sizeof(u64));
}

u64 ResidentSet::next_pow2(u64 x) {
	return 1<<(64-__builtin_clzl(x-1));
}

u64 ResidentSet::hash(u64 k) {
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

bool ResidentSet::insert(u64 pid) {
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

bool ResidentSet::contains(u64 pid) {
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

bool ResidentSet::remove(u64 pid) {
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

template<class Fn> void ResidentSet::iterateClockBatch(u64 batch, Fn fn, PageLists *pl) {
      u64 pos, newPos;
      do {
         pos = clockPos.load();
         newPos = (pos+batch) % count;
      } while (!clockPos.compare_exchange_strong(pos, newPos));

      for (u64 i=0; i<batch; i++) {
         u64 curr = ht[pos].pid.load();
         if ((curr != tombstone) && (curr != empty))
            fn(curr, pl);
         pos = (pos + 1) & mask;
      }
   }

void ResidentSet::print(){
    for(u64 i=0; i<count; i++){
        if(ht[i].pid.load() != empty && ht[i].pid.load() != tombstone){
            printf("pid: %lu\n", ht[i].pid.load());
        }
    }
}

uCache::uCache(u64 physSize, int batch) : totalPhysSize(physSize), batch(batch){
    // if all pages are mapped on the 4th level of the PT
    u64 maxPhysCount = physSize/sizeSmallPage;
   	residentSet.init(maxPhysCount);
    vmaTree = new VMATree();

   	ns = unvme_openq(sched::cpus.size(), maxQueueSize);
    initIOtoolkits();
    initYmaps();
   
   	usedPhysSize = 0;
   	readSize = 0;
   	writeSize = 0;
    pfCount = 0;
    eviction_batch_func = default_transparent_eviction;

    cout << "uCache initialized with " << physSize/(1024ull*1024*1024) << " GB of physical memory available" << endl;
}

uCache::~uCache(){};

void uCache::addVMA(u64 virtSize, u64 pageSize){
    VMA* vma = new VMA(virtSize, pageSize);
    vmaTree->addVMA(vma);
    cout << "Added a vm_area @ " << vma->start << " of size: " << virtSize/(1024ull*1024*1024) << "GB (upper bound: " << vma->start+virtSize << "), with pageSize: " << pageSize << endl;
}

bool uCache::handleFault(void* faultingAddr, exception_frame *ef){
    VMA* vma = vmaTree->getVMA(faultingAddr);
    if(vma == NULL){
        return false;
    }
    printf("shouldn't happen: %p\n", faultingAddr);
    void* basePage = alignPage(faultingAddr, vma->pageSize);
    int indexFaultingPage = (int)((u64)faultingAddr - (u64)basePage)/vma->pageSize;
    Buffer buffer(basePage, vma->pageSize);
    if(buffer.snapshotState == BufferState::Inconsistent){
        for(int i = 0; i<buffer.nb; i++){
            cout << bitset<64>(buffer.snapshot[i].word) << endl;
        }
        crash_osv();
        return false;
    }
    u64 phys;

    /*if(ef != NULL){
        page_fault_error_code ec = page_fault_error_code(ef->error_code);
        if(ec.write == 1 && bundle.getPresent() == bundle.nb){
            printf("write access to unwritable page\n");
            crash_osv();
        }
    }*/

    // another core already solved the fault -> early exit
    if(buffer.at(indexFaultingPage).present==1){
        assert(buffer.basePTE().phys != 0);
        return true;
    }
    
    if(buffer.snapshotState == BufferState::SoftUnmapped){
    // if the basePTE is still mapped to a frame 
    // try to handle softfault before handling hardfault
    // tryHandleSoftFault returns true if the goal has been reached (by this thread or another)
    // if a concurrent evictor won the race, then fall back to handling a hard fault 
        if(buffer.tryHandlingSoftFault())
            return true;
    }

    ensureFreePages(vma->pageSize);
    buffer.updateSnapshot();
    
    // another core solved the fault while we were evicting
    if(buffer.at(indexFaultingPage).present==1){
        // note: this condition is slightly relaxed compared to the BufferState checks
        // the tryMapPhys function can be ongoing and we still take this path if the required page fault has been resolved
        assert(buffer.basePTE().phys != 0);
        return true;
    }

    // get a physical frame
    // computeOrder cannot fail since we check vma->pageSize at creation
    phys = ymap_getPage(computeOrder(vma->pageSize));
    Buffer tempBuffer(get_IOtoolkit().tempPage, vma->pageSize);
    tempBuffer.map(phys);
    readPageAt(basePage, get_IOtoolkit().tempPage, vma->pageSize);
    if(buffer.tryMapPhys(phys)){ // thread that won the race
        readSize+=vma->pageSize;
        pfCount++;
        usedPhysSize+=vma->pageSize;
        assert(residentSet.insert(vma->getPID(basePage)));
    }else{
        ymap_putPage(phys, computeOrder(vma->pageSize)); // put back unused candidates
    }
    // no need to unmap since we will only R/W from it after overwriting the frame
    tempBuffer.invalidateTLBEntries();
    return true;
}

void default_transparent_eviction(PID pid, PageLists* pl){
    VMA* vma = uCacheManager->vmaTree->vma;
    Buffer buffer(uCacheManager->toPtr(pid), vma->pageSize); // TODO: this only works with a single VMA
    if(buffer.getAccessed() == 0){ // if not accessed since it was cleared
        //printf("not accessed\n");
        if(buffer.snapshotState == BufferState::SoftUnmapped){
            //printf("not present\n");
            if(uCacheManager->residentSet.remove(pid)){ // remove it from the RS to take "ownership" of the eviction
                //printf("removed %lu\n", pid);
                u16 writeBitset = buffer.getDirty();
                //std::cout << std::bitset<16>(writeBitset) << std::endl;
                for(int i=0; i<buffer.nb; i++){
                    u16 mask = 1 << i;
                    if((writeBitset & mask) != 0){
                        pl->toWrite->push_back(buffer.baseVirt+i*sizeSmallPage);
                    }
                }
                pl->toEvict->push_back(pid);
                return;
            }else{
                return;
            // if the removal failed just return
            }
        }else{ // present == 1
            buffer.trySoftUnmap(); // don't care if it fails
        }
    }else{ // accessed == 1
        buffer.tryClearAccessed(); // don't care if it fails
    }
}

void uCache::evict(){
    vector<PID> toEvict; // TODO: should this be a list of bundle ? we reconstruct the bundles at least twice during the execution
    toEvict.reserve(batch);
    vector<PID> aborted;
    aborted.reserve(batch);
    vector<void*> toWrite;
    toWrite.reserve(batch*vmaTree->vma->pageSize);

    PageLists pl;
    pl.toEvict = &toEvict;
    pl.toWrite = &toWrite;
    // 0. find candidates, lock dirty ones in shared mode
    while (toEvict.size() < batch) {
        residentSet.iterateClockBatch(batch, eviction_batch_func, &pl);
        //printf("size: %lu\n", toEvict.size());
    }

    // write single pages that are dirty.
    assert(toWrite.size() <= maxQueueSize);
    flush(toWrite, &aborted);
    // toWrite now contains only the pages that have been written
    
    // checking if the page have been remapped only improve performance
    // we need to settle on which pages to flush from the TLB at some point anyway
    // since we need to batch TLB eviction
    u64 indexAborted = 0; // toWrite were inserted in the same order as toEvict so aborted will be also
    vector<void*> toEvictAddresses;
    toEvictAddresses.reserve(toEvict.size() * vmaTree->vma->pageSize);
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
        if(indexAborted < aborted.size() && aborted.at(indexAborted) == pid){
            indexAborted++;
            return true; // the page is already in the RS
        }
        Buffer buffer(vmaTree->vma->getPtr(pid), vmaTree->vma->pageSize);
        if(buffer.snapshotState == BufferState::Mapped){
            assert(residentSet.insert(pid)); // return the page to the RS
            return true;
        }else{
            for(int i=0; i<buffer.nb; i++){
                //void* addr = buffer.huge ? buffer.baseVirt+i*sizeHugePage : buffer.baseVirt+i*sizeSmallPage;
                toEvictAddresses.push_back(buffer.baseVirt+i*sizeSmallPage);
            }
            return false;
        }
    }), toEvict.end());
    assert(indexAborted == aborted.size()); // make sure we consume everything in aborted

    if(toEvictAddresses.size() < 64){
        mmu::invlpg_tlb_all(toEvictAddresses);
    }else{
        mmu::flush_tlb_all();
    }

    // now all accesses will trigger a page fault 
     
    u64 actuallyEvictedSize = 0;
    for(PID pid: toEvict){
        Buffer buffer(vmaTree->vma->getPtr(pid), vmaTree->vma->pageSize);
        if(buffer.snapshotState == BufferState::SoftUnmapped){ // no other thread remapped the page in the mean time
            u64 phys = buffer.tryUnmapPhys();
            if(phys != 0){
                // after this point the page has completely left the cache and any access will trigger 
                // a whole new allocation
                ymap_putPage(phys, computeOrder(vmaTree->vma->pageSize)); // send back the physical page to the pool
                actuallyEvictedSize += vmaTree->vma->pageSize;
            }else{
                assert(residentSet.insert(pid));
            }
        }else{
            assert(residentSet.insert(pid)); // return the page to the RS
        }
    }
    usedPhysSize -= actuallyEvictedSize;
}

void uCache::ensureFreePages(u64 additionalSize) {
    if (usedPhysSize+additionalSize >= totalPhysSize*0.95)
        evict();
}

void uCache::readPage(PID pid){
    int ret = unvme_read(ns, get_IOtoolkit().id, toPtr(pid), vmaTree->vma->getStorageLocation(pid), 4096/blockSize);
    assert(ret==0);
}

void uCache::readPage(void* addr, u64 size) { 
    readPageAt(addr, addr, size);
    readSize+=size;
}

void uCache::readPageAt(void* addr, void* virt, u64 size) {
    int ret = unvme_read(ns, get_IOtoolkit().id, virt, vmaTree->vma->getStorageLocation(addr), size/blockSize);
    assert(ret==0);
}

void uCache::fix(PID pid){
    printf("shouldn't happen\n");
    VMA* vma = vmaTree->vma;
    void* addr = vma->getPtr(pid);
    Buffer buffer(addr, vma->pageSize);
    ensureFreePages(vma->pageSize);
    u64 phys = ymap_getPage(computeOrder(vma->pageSize));
    assert(buffer.tryMapPhys(phys));
    readPage(addr, vma->pageSize);
    pfCount++;
    residentSet.insert(pid);
    usedPhysSize += vma->pageSize;
}

void uCache::flush(std::vector<PID> toWrite){
    for(u64 i=0; i<toWrite.size(); i++){
        PID pid = toWrite[i];
        get_IOtoolkit().io_descriptors[i] = unvme_awrite(ns, get_IOtoolkit().id, toPtr(pid), vmaTree->vma->getStorageLocation(pid), sizeSmallPage/blockSize);
        assert(get_IOtoolkit().io_descriptors[i]);
    }
    for(u64 i=0; i<toWrite.size(); i++){
        int ret=unvme_apoll(get_IOtoolkit().io_descriptors[i], 16);
        if(ret!=0){
            std::cout << "IO error, writing with ret " << ret << ", queue: " << get_IOtoolkit().id << ", i " << i << ", pid " << toWrite.at(i) << std::endl;
        }
        assert(ret==0);
    }
}

void uCache::flush(std::vector<void*> toWrite, std::vector<PID>* aborted){
    int iod_used=0;
    toWrite.erase(std::remove_if(toWrite.begin(), toWrite.end(), [&](void* addr){
        if(trySetClean(addr)){
            get_IOtoolkit().io_descriptors[iod_used] = unvme_awrite(ns, get_IOtoolkit().id, addr, vmaTree->vma->getStorageLocation(addr), sizeSmallPage/blockSize);
            assert(get_IOtoolkit().io_descriptors[iod_used]);
            iod_used++;
            return false;
        }else{
            // abort writing
            u64 pid = vmaTree->vma->getPID(addr);
            if(aborted->size() == 0 || pid != aborted->at(aborted->size()-1)){
                assert(residentSet.insert(pid)); // return the page to the RS
                aborted->push_back(pid);
            }
            return true;
        }
    }), toWrite.end());
    for(int i=0; i<iod_used; i++){
        int ret=unvme_apoll(get_IOtoolkit().io_descriptors[i], 16);
        if(ret!=0){
            std::cout << "IO error, writing with ret " << ret << ", queue: " << get_IOtoolkit().id << ", i " << i << ", pid " << toWrite.at(i) << std::endl;
        }
        assert(ret==0);
    }
    writeSize += iod_used*sizeSmallPage;
}
