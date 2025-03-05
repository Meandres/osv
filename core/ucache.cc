#include <osv/ucache.hh>
#include <osv/mmu.hh>
#include <osv/mempool.hh>
#include <osv/sched.hh>

#include <vector>
#include <thread>
#include <bitset>
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>

using namespace std;

namespace ucache {

rwlock_t& vma_lock(const uintptr_t addr){ return mmu::sb_mgr->vma_lock(addr); }
rwlock_t& free_ranges_lock(const uintptr_t addr){ return mmu::sb_mgr->free_ranges_lock(addr); }

boost::optional<mmu::vma*> find_intersecting_vma(const uintptr_t addr){
	auto v = mmu::sb_mgr->find_intersecting_vma(addr);
	if(v == mmu::sb_mgr->vma_end_iterator(addr))
		return boost::none;
    return &*v;
}

std::vector<mmu::vma*> find_intersecting_vmas(const uintptr_t addr, const u64 size){
	std::vector<mmu::vma*> res;

	auto range = mmu::sb_mgr->find_intersecting_vmas(addr_range(addr, addr + size));
	for (auto i = range.first; i != range.second; ++i) {
    	res.push_back(&*i);
    }
    return res;
}

void insert(mmu::vma* v){ mmu::sb_mgr->insert(v); }
void erase(mmu::vma& v){ mmu::sb_mgr->erase(v); }

bool validate(const uintptr_t addr, const u64 size){ return mmu::sb_mgr->validate_map_fixed(addr, size); }
void allocate_range(const uintptr_t addr, const u64 size){ mmu::sb_mgr->allocate_range(addr, size); }
uintptr_t reserve_range(const u64 size, size_t alignment){ return mmu::sb_mgr->reserve_range(size); }
void free_range(const uintptr_t addr, const u64 size){ mmu::sb_mgr->free_range(addr, size); }

/* Walks the page table and allocates pt elements if necessary.
 * Operates in a range between [virt, virt+size-page_size]
 * 
 * 			huge
 *  		0  	1
 * init 0	1	0
 *			1	1	1
 *	Level 1 pages (related to 2MiB pages shouuld be initialized in all the cases
 *	except when huge is true and init is false
 */
void allocate_pte_range(void* virt, u64 size, bool init, bool huge){
	PTE emptypte = pt_elem::make(0ull, false);
	bool initHuge = !huge || init;
  unsigned id3 = idx(virt, 3), id2 = idx(virt, 2), id1 = idx(virt, 1), id0 = idx(virt, 0);
  void* end = huge ? virt + size - 2*1024*1024 : virt + size - 4096; // bound included in the interval
	unsigned end_id3 = idx(end, 3), end_id2 = idx(end, 2), end_id1 = idx(end, 1), end_id0 = idx(end, 0);
	virt_addr ptRoot = mmu::phys_cast<u64>(processor::read_cr3());
	for(unsigned i3 = id3; i3 <= end_id3; i3++){
			virt_addr l3 = ensure_valid_pt_elem(ptRoot, i3, true);
			unsigned i2_start = i3 == id3 ? id2 : 0;
			unsigned i2_end = i3 == end_id3 ? end_id2: 511;
			for(unsigned i2 = i2_start; i2 <= i2_end; i2++){
					virt_addr l2 = ensure_valid_pt_elem(l3, i2, true);
					unsigned i1_start = i3 == id3 && i2 == id2 ? id1 : 0;
					unsigned i1_end = i3 == end_id3 && i2 == end_id2 ? end_id1: 511;
					for(unsigned i1 = i1_start; i1 <= i1_end; i1++){
							virt_addr l1 = ensure_valid_pt_elem(l2, i1, initHuge, huge);
							if(!huge){
								unsigned i0_start = i3 == id3 && i2 == id2 && i1 == id1 ? id0 : 0;
								unsigned i0_end = i3 == end_id3 && i2 == end_id2 && i1 == end_id1 ? end_id0: 511;
								for(unsigned i0 = i0_start; i0 <= i0_end; i0++){
										ensure_valid_pte(l1, i0, init);
										if(!init){
											virt_addr l0 = l1+i0;
											if(*l0 != emptypte.word){
												std::cout << std::bitset<64>(*l0) << std::endl;
											}
											assert(*l0 == emptypte.word);
										}
								}
							} 
					}
			}
	}
}

Buffer::Buffer(void* addr, u64 size, VMA* vma_ptr){
	baseVirt = addr;
	u64 workingPageSize = mmu::page_size;
	if(vma_ptr == NULL){
		printf("Buffer created with vma == NULL. You probably don't want this unless you know what you are doing.\n");
	}
	vma = vma_ptr;
	/*if(size%sizeHugePage == 0){
		workingPageSize = sizeHugePage;
		huge = true;
	}*/
	nb = size / workingPageSize;
	for(int i = 0; i < nb; i++){
		/*if(huge)
			pteRefs[i] = walkRefHuge(addr + (i * workingPageSize));
		else*/
		pteRefs[i] = walkRef(addr + (i* workingPageSize));
	}
	updateSnapshot();
}

bool Buffer::tryHandlingSoftFault(){
	assert(snapshotState == BufferState::SoftUnmapped); // should only be called in that case
	for(int i = 0; i < nb; i++){
		retry:
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 1;
		if(!pteRefs[i]->compare_exchange_strong(snapshot[i].word, newPTE.word)){
			PTE updatedPTE = PTE(*pteRefs[i]);
			// we need to handle the case where another core accessed one of the page
			if(updatedPTE.phys == snapshot[i].phys && updatedPTE.present == snapshot[i].present){
				snapshot[i].accessed = updatedPTE.accessed;
				snapshot[i].dirty = updatedPTE.dirty;
				goto retry;
			}
			return false;
		}
	}
	return true;
}

void Buffer::tryClearAccessed(){
	for(int i = 0; i < nb; i++){ // just try to 
		PTE pte = snapshot[i];
		if(pte.accessed == 0){ // simply skip
			continue;
		}
		PTE newPTE = PTE(pte.word);
		newPTE.accessed = 0; 
		pteRefs[i]->compare_exchange_strong(pte.word, newPTE.word); // best effort 
	}
}

bool Buffer::trySoftUnmap(){
	assert(snapshotState == BufferState::Mapped); // should only be called in that case
	for(int i = 0; i < nb; i++){
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 0;
		if(!pteRefs[i]->compare_exchange_strong(snapshot[i].word, newPTE.word)){
			// in that case, accessed means aborting the soft unmap so no second chance
			return false;
		}
	}
	return true;
}

// called to handle a hard fault. Sets the pages to present and set the frame accordingly
// the core that wins the race gets to use its frame
bool Buffer::tryMapPhys(u64 phys){
	assert(snapshotState == BufferState::HardUnmapped); // should only be called in that case
	for(int i = 0; i < nb; i++){
		//retry:
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 1;
		newPTE.phys = phys+i;
		if(!pteRefs[i]->compare_exchange_strong(snapshot[i].word, newPTE.word)){
			std::cout << std::bitset<64>(snapshot[i].word) << "\n" << std::bitset<64>(PTE(*(pteRefs[i])).word) << std::endl;
			/*PTE updatedPTE = PTE(*pteRefs[i]);
			// we need to handle the case where another core accessed one of the page
			if(updatedPTE.phys == snapshot[i].phys && updatedPTE.present == snapshot[i].present){
				snapshot[i].accessed = updatedPTE.accessed;
				snapshot[i].dirty = updatedPTE.dirty;
				goto retry;
			}*/
			return false;
		}
	}
	return true;
}

int Buffer::getPresent(){
	int acc = 0;
	for(int i=0; i<nb; i++){
		acc += snapshot[i].present;
	}
	return acc;
}

int Buffer::getAccessed(){
	int acc = 0;
	for(int i=0; i<nb; i++){
		acc += snapshot[i].accessed;
	}
	return acc;
}

u16 Buffer::getDirty(){
	u16 bitset = 0;
	for(int i=0; i<nb; i++){
		if(snapshot[i].dirty == 1){
			bitset |= 1 << i;
		}
	}
	return bitset;
}

u64 Buffer::tryUnmapPhys(){
	PTE base = basePTE();
	if(base.phys == 0){ // evictors own the eviction so this cannot happen
		printf("phys is 0 in unmap\n");
		crash_osv();
	}
	
	/*if(base.present == 1){
		printf("concurrent fault\n");
		return 0; // another core soft faulted the page
	}*/
	u64 phys = base.phys;
	PTE newPTE = PTE(base.word);
	newPTE.present = 0;
	newPTE.phys = 0;
	std::atomic<u64>* baseRef = pteRefs[0];
	if(baseRef->compare_exchange_strong(base.word, newPTE.word)){
		return phys;
	}
	std::cout << "old: " << std::bitset<64>(base.word) << "\nnew: " << std::bitset<64>(PTE(*baseRef).word) << std::endl;
	return 0;
}

// this function does not race with any other thread
void Buffer::map(u64 phys){
	for(int i=0; i<nb; i++){
		PTE newPTE = snapshot[i];
		newPTE.present = 1;
		newPTE.phys = phys+i;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
}

// this function does not race with any other thread
u64 Buffer::unmap(){
	u64 phys;
	PTE newPTE = basePTE();
	phys = newPTE.phys;
	for(int i=0; i<nb; i++){
		newPTE = snapshot[i];
		newPTE.present = 0;
		newPTE.phys = 0;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
	return phys;
}
// this function does not race with any other thread

void Buffer::invalidateTLBEntries(){
	for(int i=0; i<nb; i++){
		//u64 workingSize = huge ? sizeHugePage : sizeSmallPage;
		invalidateTLBEntry(baseVirt+i*mmu::page_size);
	}
}

// request a memory region and registers a VMA with the rest of the system. 
// init controls whether the ptes should be initialized with frames or not
// huge controls the size of the pages to initialize (4KiB or 2MiB)
static void* createVMA(u64 id, u64 size, size_t alignment, bool init, bool huge) {
	uintptr_t p = reserve_range(size);
	allocate_pte_range((void*)p, size, init, huge);
	mmu::vma* vma = new mmu::anon_vma(addr_range(p, p+size), mmu::perm_rwx, 0, id);
	WITH_LOCK(vma_lock(p).for_write()){
		insert(vma);
	}
	return (void*)p;
}
/*
static void removeVMA(void* start, u64 size){
	return; // TODO: implement this
}
*/
uCache* uCacheManager;

// contains objects necessary to do IO operations
// a single structure is easier to do PERCPU/__thread depending
struct IOtoolkit {
    void* tempPage;
    int id;
    unvme_iod_t io_descriptors[maxQueueSize];

    IOtoolkit(int i){
        tempPage = createVMA(0, mmu::page_size, mmu::page_size, true, false); // this only works for buffer size = 4KiB
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
    for(auto c: sched::cpus){
        auto *ptp = percpu_IOtoolkit.for_cpu(c);
        *ptp = new IOtoolkit(c->id);
    }
}

HashTableResidentSet::HashTableResidentSet(u64 maxCount){
	count = next_pow2(maxCount * 1.5);
	mask = count-1;
	clockPos = 0;
	ht = (Entry*)mmap(NULL, count * sizeof(Entry), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	madvise((void*)ht, count * sizeof(Entry), MADV_HUGEPAGE);
	memset((void*)ht, 0xFF, count * sizeof(Entry));
}

HashTableResidentSet::~HashTableResidentSet() {
    munmap(ht, count * sizeof(u64));
}

u64 HashTableResidentSet::next_pow2(u64 x) {
	return 1<<(64-__builtin_clzl(x-1));
}

u64 HashTableResidentSet::hash(u64 k) {
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

bool HashTableResidentSet::insert(u64 pid) {
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

bool HashTableResidentSet::contains(u64 pid) {
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

bool HashTableResidentSet::remove(u64 pid) {
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

u64 HashTableResidentSet::getNextBatch(u64 batch){
	u64 pos, newPos;
    do {
    	pos = clockPos.load();
        newPos = (pos+batch) % count;
    } while (!clockPos.compare_exchange_strong(pos, newPos));
	return pos;
}

void HashTableResidentSet::getNextValidEntry(std::pair<u64, Entry*>* entry){
    for(;;){
    	entry->second->pid = ht[entry->first].pid.load();
        if ((entry->second->pid != tombstone) && (entry->second->pid != empty))
    		return;	
        entry->first = (entry->first + 1) & mask;
    }
}

static u64 nextVMAid = 1; // 0 is reserved for other vmas

VMA::VMA(u64 size, u64 page_size, u64 first_lba, isDirty_func dirty_func, setClean_func clean_func, ResidentSet* set, 
		vm_to_storage_mapping_func f, alloc_func prefetch_policy, evict_func evict_policy):
	size(size), lba_start(first_lba), pageSize(page_size), id(nextVMAid++), residentSet(set), vm_storage_map(f),
	isDirty_implem(dirty_func), setClean_implem(clean_func), prefetch_pol(prefetch_policy), evict_pol(evict_policy)
{
	assert(isSupportedPageSize(page_size));
	bool huge = page_size == 2ul*1024*1024 ? true : false;
	start = createVMA(id, size, page_size, false, huge);
}

VMA::VMA(u64 size, u64 page_size, u64 first_lba):
VMA(size, mmu::page_size, first_lba, pte_isDirty, pte_setClean, new HashTableResidentSet(uCacheManager->totalPhysSize/page_size), linear_mapping, default_prefetch, default_transparent_eviction)
{
}

uCache::uCache(u64 physSize, int batch) : totalPhysSize(physSize), batch(batch){
   	ns = unvme_openq(sched::cpus.size(), maxQueueSize);
    initIOtoolkits();
   
   	usedPhysSize = 0;
   	readSize = 0;
   	writeSize = 0;

    cout << "uCache initialized with " << physSize/(1024ull*1024*1024) << " GB of physical memory available" << endl;
}

uCache::~uCache(){};

void* uCache::addVMA(u64 virtSize, u64 pageSize, u64 first_lba){
    VMA* vma = new VMA(virtSize, pageSize, first_lba);
    vmas.insert({vma->id, vma});
    cout << "Added a vm_area @ " << vma->start << " of size: " << virtSize/(1024ull*1024*1024) << "GB, with pageSize: " << pageSize << endl;
		return vma->start;
}

VMA* uCache::getVMA(void* start_vma){
	for(auto p: vmas){
		if(p.second->start == start_vma){
			return p.second;
		}
	}
	return NULL;
}

void uCache::handleFault(u64 vmaID, void* faultingAddr, exception_frame *ef){
    auto search = vmas.find(vmaID);
	assert(search != vmas.end());
	VMA* vma = search->second;
    
	void* basePage = alignPage(faultingAddr, vma->pageSize);
    int indexFaultingPage = (int)((u64)faultingAddr - (u64)basePage)/vma->pageSize;
    Buffer buffer(basePage, vma->pageSize, vma);
    if(buffer.snapshotState == BufferState::Inconsistent){
        for(int i = 0; i<buffer.nb; i++){
            cout << bitset<64>(buffer.snapshot[i].word) << endl;
        }
        crash_osv();
    }
    phys_addr phys;

    // another core already solved the fault -> early exit
    if(buffer.at(indexFaultingPage).present==1){
        assert(buffer.basePTE().phys != 0);
        return;
    }
    
    if(buffer.snapshotState == BufferState::SoftUnmapped){
    // if the basePTE is still mapped to a frame 
    // try to handle softfault before handling hardfault
    // tryHandleSoftFault returns true if the goal has been reached (by this thread or another)
    // if a concurrent evictor won the race, then fall back to handling a hard fault 
        if(buffer.tryHandlingSoftFault())
            return;
    }

    ensureFreePages(vma->pageSize);
    buffer.updateSnapshot();
    
    // another core solved the fault while we were evicting
    if(buffer.at(indexFaultingPage).present==1){
        // note: this condition is slightly relaxed compared to the BufferState checks
        // the tryMapPhys function can be ongoing and we still take this path if the required page fault has been resolved
        assert(buffer.basePTE().phys != 0);
        return;
    }

    // get a physical frame
    // computeOrder cannot fail since we check vma->pageSize at creation
    phys = frames_alloc_phys_addr(vma->pageSize);
	// TODO: add prefetching here
    Buffer tempBuffer(get_IOtoolkit().tempPage, vma->pageSize, NULL);
    tempBuffer.map(phys);
    readBufferToTmp(&buffer, &tempBuffer);
    if(buffer.tryMapPhys(phys)){ // thread that won the race
        readSize += vma->pageSize;
        usedPhysSize += vma->pageSize;
				vma->usedPhysSize += vma->pageSize;
        assert(vma->residentSet->insert(vma->getPID(basePage)));
    }else{
			frames_free_phys_addr(phys, vma->pageSize); // put back unused candidates
    }
    // no need to unmap since we will only R/W from it after overwriting the frame
    tempBuffer.invalidateTLBEntries();
}

void default_transparent_eviction(VMA* vma, u64 nbToEvict, evictList el){
    while (el.size() < nbToEvict) {
			u64 id = vma->residentSet->getNextBatch(nbToEvict);
			ResidentSet::Entry* entry = nullptr;
			for(u64 i = 0; i<nbToEvict; i++){
				std::pair<u64, ResidentSet::Entry*> pair({id, entry});
				vma->residentSet->getNextValidEntry(&pair);
				u64 pid = entry->pid;
    		Buffer* buffer = new Buffer(vma->getPtr(pid), vma->pageSize, vma);
    		if(buffer->getAccessed() == 0){ // if not accessed since it was cleared
        	if(buffer->snapshotState == BufferState::SoftUnmapped){
          	if(vma->residentSet->remove(pid)){ // remove it from the RS to take "ownership" of the eviction
                el.push_back(buffer);
								continue;
						}
        	}else // present == 1
          	buffer->trySoftUnmap(); // don't care if it fails
    		}else // accessed == 1
        	buffer->tryClearAccessed(); // don't care if it fails
				delete buffer;
			}
  	}
}

void uCache::evict(){
	std::vector<Buffer*> toEvict;
    toEvict.reserve(batch);

		// 0. find candidates
		for(auto p: vmas){
			p.second->chooseEvictionCandidates(batch/vmas.size(), toEvict);
		}

    // write single pages that are dirty.
    assert(toEvict.size() <= maxQueueSize);
    flush(toEvict);
    
    // checking if the page have been remapped only improve performance
    // we need to settle on which pages to flush from the TLB at some point anyway
    // since we need to batch TLB eviction
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](Buffer* buf) {
				buf->updateSnapshot();
        if(buf->snapshotState == BufferState::Mapped){
            assert(buf->vma->residentSet->insert(buf->vma->getPID(buf->baseVirt))); // return the page to the RS
						delete buf;
            return true;
        }
				return false;
    }), toEvict.end());

    if(toEvict.size() < THRESHOLD_INVLPG_FLUSH){
        invlpg_tlb_all(toEvict);
    }else{
        mmu::flush_tlb_all();
    }

    // now all accesses will trigger a page fault 
     
    u64 actuallyEvictedSize = 0;
    for(Buffer* buf: toEvict){
				buf->updateSnapshot();
        if(buf->snapshotState == BufferState::SoftUnmapped){ // no other thread remapped the page in the mean time
            u64 phys = buf->tryUnmapPhys();
            if(phys != 0){
                // after this point the page has completely left the cache and any access will trigger 
                // a whole new allocation
								frames_free_phys_addr(phys, buf->vma->pageSize); // put back unused candidates
                actuallyEvictedSize += buf->vma->pageSize;
								// TODO: need to remove the size from the accounting of the VMA too. 
								// need to batch it tho. Accumulate then remove at the end of the for loop
            }else
                assert(buf->vma->residentSet->insert(buf->vma->getPID(buf->baseVirt)));
								delete buf;
        }else
        		assert(buf->vma->residentSet->insert(buf->vma->getPID(buf->baseVirt))); // return the page to the RS
						delete buf;
    }
    usedPhysSize -= actuallyEvictedSize;
}

void uCache::ensureFreePages(u64 additionalSize) {
    if (usedPhysSize+additionalSize >= totalPhysSize*0.95)
        evict();
}

void uCache::readBuffer(Buffer* buf){
		assert(buf->vma != NULL);
    int ret = unvme_read(ns, get_IOtoolkit().id, buf->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/blockSize);
    assert(ret==0);
		readSize += buf->vma->pageSize;
}

void uCache::readBufferToTmp(Buffer* buf, Buffer* tmp) {
		assert(buf->vma != NULL);
    int ret = unvme_read(ns, get_IOtoolkit().id, tmp->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/blockSize);
    assert(ret==0);
}

/*void uCache::fix(PID pid){
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
}*/

void uCache::flush(std::vector<Buffer*> toWrite){
		int iod_used = 0;
    for(u64 i=0; i<toWrite.size(); i++){
        Buffer* buf = toWrite[i];
				buf->updateSnapshot();
				for(u16 j = 0; j<buf->nb; j++){ // TODO: this only works for multiples of 4KiB
					if(buf->vma->isDirty(buf, j)){
        		get_IOtoolkit().io_descriptors[iod_used] = unvme_awrite(ns, sched::cpu::current()->id, buf->baseVirt+j*mmu::page_size, buf->vma->getStorageLocation(buf->baseVirt+j*mmu::page_size), mmu::page_size/blockSize);
        		assert(get_IOtoolkit().io_descriptors[iod_used]); // TODO: we might run out of iod if we only use large pages
						iod_used++;
						buf->vma->setClean(buf, j); // TODO: check if setClean should be before or after the write
					}
				}
    }
    for(int i=0; i<iod_used; i++){
        int ret=unvme_apoll(get_IOtoolkit().io_descriptors[i], 16);
        if(ret!=0){
            std::cout << "IO error, writing with ret " << ret << ", queue: " << get_IOtoolkit().id << std::endl;
        }
        assert(ret==0);
    }
    writeSize += iod_used*mmu::page_size;
}

}

