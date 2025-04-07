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

u64 next_available_slba=0;
int next_available_file_id=0;
std::vector<ucache_file*> available_nvme_files;

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
										initialize_pte(l1, i0, init);
										if(!init){
											virt_addr l0 = l1+i0;
											if(*l0 != emptypte.word){
												printf("l0: %p\n", l0);
												printf("leaf not initialized properly\n");
												printf("%lu %lu %lu %lu\n%lu %lu %lu %lu\n%lu %lu %lu %lu\n", id3, id2, id1, id0, end_id3, end_id2, end_id1, end_id0, i3, i2, i1, i0);
												std::cout << std::bitset<64>(*(l1-1)) << std::endl;
												std::cout << std::bitset<64>(*l0) << std::endl;
												std::cout << std::bitset<64>(*(l1+1)) << std::endl;
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
	vma = vma_ptr;
	size_t nb = size / workingPageSize;
	pteRefs.reserve(nb);
	snapshot.reserve(nb);
	for(size_t i = 0; i < nb; i++){
		pteRefs.push_back(walkRef(addr + (i* workingPageSize)));
		snapshot.push_back(PTE(0));
	}
	updateSnapshot();
}

bool Buffer::tryHandlingSoftFault(){
	for(size_t i = 0; i < pteRefs.size(); i++){
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
	for(size_t i = 0; i < pteRefs.size(); i++){ // just try to 
		PTE pte = snapshot[i];
		if(pte.accessed == 0){ // simply skip
			continue;
		}
		PTE newPTE = PTE(pte.word);
		newPTE.accessed = 0; 
		pteRefs[i]->compare_exchange_strong(pte.word, newPTE.word); // best effort 
	}
}

// called to handle a hard fault. Sets the pages to present and set the frame accordingly
// the core that wins the race gets to use its frame
bool Buffer::tryMapPhys(u64 phys){
	if(snapshotState.load() != BufferState::Resolving){
		return false;
	}
	for(size_t i = 0; i < pteRefs.size(); i++){
		//retry:
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 1;
		newPTE.phys = phys+i;
		if(!pteRefs[i]->compare_exchange_strong(snapshot[i].word, newPTE.word)){
			std::cout << std::bitset<64>(snapshot[i].word) << "\n" << std::bitset<64>(PTE(*(pteRefs[i])).word) << "\n" << std::bitset<64>(phys) << std::endl;
			return false;
		}
	}
	return true;
}

int Buffer::getPresent(){
	int acc = 0;
	for(size_t i=0; i<pteRefs.size(); i++){
		acc += snapshot[i].present;
	}
	return acc;
}

int Buffer::getAccessed(){
	int acc = 0;
	for(size_t i=0; i<pteRefs.size(); i++){
		acc += snapshot[i].accessed;
	}
	return acc;
}

u16 Buffer::getDirty(){
	u16 bitset = 0;
	for(size_t i=0; i<pteRefs.size(); i++){
		if(snapshot[i].dirty == 1){
			bitset |= 1 << i;
		}
	}
	return bitset;
}

u64 Buffer::tryUnmapPhys(){
	if(snapshotState.load() != BufferState::Evicting){
		return 0;
	}
	u64 phys = snapshot[0].phys;
	for(size_t i = 0; i < pteRefs.size(); i++){
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 0;
		newPTE.phys = 0;
		if(!pteRefs[i]->compare_exchange_strong(snapshot[i].word, newPTE.word)){
			return 0;
		}
	}
	return phys;
}

// this function does not race with any other thread
void Buffer::map(u64 phys){
	for(size_t i=0; i<pteRefs.size(); i++){
		PTE newPTE = snapshot[i];
		newPTE.present = 1;
		newPTE.phys = phys+i;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
}

// this function does not race with any other thread
u64 Buffer::unmap(){
	u64 phys = snapshot[0].phys;
	for(size_t i=0; i<pteRefs.size(); i++){
		PTE newPTE = PTE(snapshot[i].word);
		newPTE.present = 0;
		newPTE.phys = 0;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
	return phys;
}
// this function does not race with any other thread

void Buffer::invalidateTLBEntries(){
	for(size_t i=0; i<pteRefs.size(); i++){
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
	WITH_LOCK(vma_lock(p).for_write()){ // TODO: remove this lock when we change the DS
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
    Buffer* tempBuffer;
    int id;
    unvme_iod_t io_descriptors[maxQueueSize];

    IOtoolkit(int i){
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

bool HashTableResidentSet::insert(Buffer* buf) {
	u64 pos = hash((uintptr_t)buf) & mask;
    while (true) {
        Buffer* curr = ht[pos].buf.load();
        if(curr == buf){
            return false;
        }
        assert(curr != buf);
        if (((uintptr_t)curr == empty) || ((uintptr_t)curr == tombstone))
            if (ht[pos].buf.compare_exchange_strong(curr, buf))
                return true;
        pos = (pos + 1) & mask;
    }
}

bool HashTableResidentSet::contains(Buffer* buf) {
    u64 pos = hash((uintptr_t)buf) & mask;
    while (true) {
        Buffer* curr = ht[pos].buf.load();
        if(curr == buf){
            return true;
        }
        assert(curr != buf);
        if (((uintptr_t)curr == empty) || ((uintptr_t)curr == tombstone))
            return false;
        pos = (pos + 1) & mask;
    }
}

bool HashTableResidentSet::remove(Buffer* buf) {
	u64 pos = hash((uintptr_t)buf) & mask;
    while (true) {
    	Buffer* curr = ht[pos].buf.load();
        if ((uintptr_t)curr == empty)
        	return false;

        if (curr == buf)
        	if (ht[pos].buf.compare_exchange_strong(curr, (Buffer*)tombstone))
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
    		entry->second->buf = ht[entry->first].buf.load();
        if ((entry->second->buf != (Buffer*)tombstone) && (entry->second->buf != (Buffer*)empty)){
    			return;	
				}
        entry->first = (entry->first + 1) & mask;
    }
}

static u64 nextVMAid = 1; // 0 is reserved for other vmas

VMA::VMA(u64 size, u64 page_size, isDirty_func dirty_func, setClean_func clean_func, ResidentSet* set, 
		struct ucache_file* f, alloc_func prefetch_policy, evict_func evict_policy):
	size(size), file(f), pageSize(page_size), id(nextVMAid++), residentSet(set), isDirty_implem(dirty_func),
	setClean_implem(clean_func), prefetch_pol(prefetch_policy), evict_pol(evict_policy)
{
	assert(isSupportedPageSize(page_size));
	bool huge = page_size == 2ul*1024*1024 ? true : false;
	start = createVMA(id, size, page_size, false, huge);
}

VMA* VMA::newVMA(u64 size, u64 page_size)
{
		printf("Creating a file at %lu with %lu blocks (of size %u)\n", next_available_slba, size/uCacheManager->ns->blocksize, uCacheManager->ns->blocksize);
    struct ucache_file* f = new ucache_file("tmp", next_available_slba, size/uCacheManager->ns->blocksize, uCacheManager->ns->blocksize);
    available_nvme_files.push_back(f);
		return new VMA(size, page_size, pte_isDirty, pte_setClean, new HashTableResidentSet(uCacheManager->totalPhysSize/page_size), f, default_prefetch, default_transparent_eviction);
}

VMA* VMA::newVMA(const char* name, u64 page_size)
{
	assert(name != NULL);
	struct ucache_file* f = find_ucache_file(name);
	return new VMA(align_up(f->size, page_size), page_size, pte_isDirty, pte_setClean, new HashTableResidentSet(uCacheManager->totalPhysSize/page_size), f, default_prefetch, default_transparent_eviction);
}

uCache::uCache(u64 physSize, int batch) : totalPhysSize(physSize), batch(batch){
		ns = unvme_openq(sched::cpus.size(), maxQueueSize);
		initIOtoolkits();

   	usedPhysSize = 0;
   	readSize = 0;
   	writeSize = 0;
		tlbFlush = 0;

    cout << "uCache initialized with " << physSize/(1024ull*1024*1024) << " GB of physical memory available" << endl;
}

uCache::~uCache(){};

void* uCache::addVMA(u64 virtSize, u64 pageSize){
    VMA* vma = VMA::newVMA(virtSize, pageSize);
    vmas.insert({vma->id, vma});
    cout << "Added a vm_area @ " << vma->start << " of size: " << virtSize/(1024ull*1024*1024) << "GB, with pageSize: " << pageSize << endl;
		return vma->start;
}

VMA* uCache::getOrCreateVMA(const char* name){
	VMA* vma;
	for(auto p: vmas){
		vma = p.second;
		if(strcmp(vma->file->name.c_str(), name) == 0){
			return vma;
		}
	}
  vma = VMA::newVMA(name, mmu::page_size);
  vmas.insert({vma->id, vma});
  cout << "Added a vm_area @ " << vma->start << " of size: " << vma->file->size << ", with pageSize: " << vma->pageSize << endl;
	return vma;
}

VMA* uCache::getVMA(void* start_vma){
	for(auto p: vmas){
		if(p.second->start == start_vma){
			return p.second;
		}
	}
	return NULL;
}

Buffer* uCache::getBuffer(void* addr){
	Buffer* ret = NULL;
	for(auto v : vmas){
		VMA* vma = v.second;
		ret = vma->getBuffer(addr);
		if(ret != NULL)
			return ret;
	}
	return ret;
}

void uCache::handleFault(u64 vmaID, void* faultingAddr, exception_frame *ef){
  auto search = vmas.find(vmaID);
	assert(search != vmas.end());
	VMA* vma = search->second;

	std::vector<Buffer*> pl; // need those here for goto to work
	BufferState oldState;
    
	void* basePage = alignPage(faultingAddr, vma->pageSize);
	Buffer* buffer = new Buffer(basePage, vma->pageSize, vma);
	if(buffer->snapshotState == BufferState::Inconsistent){
			goto out_polling;
	}
	phys_addr phys;

	oldState = buffer->snapshotState.load();
	switch(oldState){
		case BufferState::Evicting:
			if(!buffer->snapshotState.compare_exchange_strong(oldState, BufferState::Mapped)){ // try to set the state to mapped
				if(buffer->snapshotState.load() == BufferState::Mapped) // if it failed, only continue if this thread lost to the eviction, otherwise just return
					return;
			}
			break;
		case BufferState::Mapped:
			return;
			break;
		case BufferState::Prefetching:
			while(buffer->snapshotState.load() == BufferState::Prefetching){
				unvme_check_completion(ns, get_IOtoolkit().id);
			}
			readSize += vma->pageSize;
			assert(vma->residentSet->insert(buffer));
			return;
			break;
		default:
			break;
	}

	pl.reserve(batch);
	vma->choosePrefetchingCandidates(basePage, pl);
	ensureFreePages(vma->pageSize *(1 + pl.size())); // make enough room for the page being faulted and the prefetched ones
	
	buffer->updateSnapshot();
	if(buffer->snapshotState.load() == BufferState::Evicting){
		BufferState oldState = BufferState::Evicting;
		if(!buffer->snapshotState.compare_exchange_strong(oldState, BufferState::Mapped)){ // try to set the state to mapped
			if(buffer->snapshotState.load() == BufferState::Mapped){ // if it failed, only continue if this thread lost to the eviction, otherwise just return
				return;
			}
		}
	}

	oldState = buffer->snapshotState.load();
	if(oldState == BufferState::Mapped){
		return;
	}
	if(oldState == BufferState::Resolving){
		goto out_polling;
	}
	if(!buffer->snapshotState.compare_exchange_strong(oldState, BufferState::Resolving)){
		goto out_polling;
	}

	phys = frames_alloc_phys_addr(vma->pageSize);
	assert(buffer->tryMapPhys(phys));
	readBuffer(buffer);
	readSize += vma->pageSize;
	usedPhysSize += vma->pageSize;
	oldState = BufferState::Resolving;
	assert(buffer->snapshotState.compare_exchange_strong(oldState, BufferState::Mapped));
	vma->usedPhysSize += vma->pageSize;
	assert(vma->residentSet->insert(buffer));
	prefetch(vma, pl);
	return;
out_polling:
	do{
	buffer->updateSnapshot();
	} while(buffer->snapshotState.load() != BufferState::Mapped);
	delete buffer;
}

void uCache::prefetch(VMA *vma, PrefetchList pl){
	u64 prefetchedSize = 0;
	for(Buffer* buf: pl){
		buf->updateSnapshot();
		BufferState oldState = buf->snapshotState.load();
		if(oldState == BufferState::Prefetching || oldState == BufferState::Mapped){
			continue;
		}
		if(!buf->snapshotState.compare_exchange_strong(oldState, BufferState::Prefetching)){
			continue; // ignore if failed -> best effort
		}
		u64 phys = frames_alloc_phys_addr(buf->vma->pageSize);
		if(buf->tryMapPhys(phys)){ // this can fail if another thread already resolved the prefetched buffer concurrently
			unvme_aread(ns, get_IOtoolkit().id, buf->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/buf->vma->file->lba_size);
			usedPhysSize += buf->vma->pageSize;
			prefetchedSize += buf->vma->pageSize;
		}else{
			oldState = BufferState::Prefetching;
			assert(buf->snapshotState.compare_exchange_strong(oldState, BufferState::Mapped));
			frames_free_phys_addr(phys, vma->pageSize); // put back unused candidates
		}
	}
	vma->usedPhysSize += prefetchedSize;
}

void default_transparent_eviction(VMA* vma, u64 nbToEvict, EvictList el){
    while (el.size() < nbToEvict) {
			u64 id = vma->residentSet->getNextBatch(nbToEvict);
			ResidentSet::Entry entry;
			for(u64 i = 0; i<nbToEvict; i++){
				std::pair<u64, ResidentSet::Entry*> pair({id, &entry});
				vma->residentSet->getNextValidEntry(&pair);
				Buffer* buffer = entry.buf;
				buffer->updateSnapshot();
    		if(buffer->getAccessed() == 0){ // if not accessed since it was cleared
					vma->addEvictionCandidate(buffer, el);
    		}else{ // accessed == 1
        	buffer->tryClearAccessed(); // don't care if it fails
				}
			}
  	}
}

void uCache::evict(){
		std::vector<Buffer*> toEvict;
    toEvict.reserve(batch*1.5);

		// 0. find candidates
		for(const auto& p: vmas){
			p.second->chooseEvictionCandidates(batch/vmas.size(), toEvict);
		}

    // write single pages that are dirty.
    assert(toEvict.size() <= maxQueueSize);
    //flush(toEvict);
    
    // checking if the page have been remapped only improve performance
    // we need to settle on which pages to flush from the TLB at some point anyway
    // since we need to batch TLB eviction
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](Buffer* buf) {
				buf->updateSnapshot();
        if(buf->snapshotState == BufferState::Mapped){
            assert(buf->vma->residentSet->insert(buf)); // return the page to the RS
            return true;
        }
				return false;
    }), toEvict.end());

    if(toEvict.size() < THRESHOLD_INVLPG_FLUSH){
        invlpg_tlb_all(toEvict);
    }else{
        mmu::flush_tlb_all();
    }
		tlbFlush++;

    // now all accesses will trigger a page fault 
     
    u64 actuallyEvictedSize = 0;
		std::map<VMA*, u64> evictedSizePerVMA;
    for(Buffer* buf: toEvict){
				buf->updateSnapshot();
        if(buf->snapshotState == BufferState::Evicting){ // no other thread remapped the page in the mean time
            u64 phys = buf->tryUnmapPhys();
            if(phys != 0){
                // after this point the page has completely left the cache and any access will trigger 
                // a whole new allocation
								frames_free_phys_addr(phys, buf->vma->pageSize); // put back unused candidates
                actuallyEvictedSize += buf->vma->pageSize;
								buf->snapshotState.store(BufferState::Unmapped);
								if(evictedSizePerVMA.find(buf->vma) == evictedSizePerVMA.end()){
									evictedSizePerVMA[buf->vma] = buf->vma->pageSize;
								}else{
									evictedSizePerVMA[buf->vma] += buf->vma->pageSize;
								}
            }else{
            	assert(buf->vma->residentSet->insert(buf));
						}
        }else{
        		assert(buf->vma->residentSet->insert(buf)); // return the page to the RS
				}
    }
		for(const auto& p: evictedSizePerVMA){
			VMA* vma = p.first;
			vma->usedPhysSize -= p.second; 
		}
    usedPhysSize -= actuallyEvictedSize;
}

void uCache::ensureFreePages(u64 additionalSize) {
    if (usedPhysSize+additionalSize >= totalPhysSize*0.95)
        evict();
}

void uCache::readBuffer(Buffer* buf){
	assert(buf->vma != NULL);
	int ret = unvme_read(ns, get_IOtoolkit().id, buf->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/buf->vma->file->lba_size);
	assert(ret == 0);
	readSize += buf->vma->pageSize;
}

void uCache::readBufferToTmp(Buffer* buf, Buffer* tmp) {
	assert(buf->vma != NULL);
	int ret = unvme_read(ns, get_IOtoolkit().id, tmp->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/buf->vma->file->lba_size);
	assert(ret == 0);
}

void uCache::flush(std::vector<Buffer*> toWrite){
	std::vector<Buffer*> requests;
	u64 sizeWritten = 0;
	requests.reserve(batch+5);
	for(u64 i=0; i<toWrite.size(); i++){
		Buffer* buf = toWrite[i];
		buf->updateSnapshot();
		if(buf->vma->isDirty(buf)){ // this writes the whole buffer
			BufferState prev = BufferState::Evicting;
			assert(buf->snapshotState.compare_exchange_strong(prev, BufferState::Writing));
			unvme_iod_t iod = unvme_awrite(ns, get_IOtoolkit().id, buf->baseVirt, buf->vma->getStorageLocation(buf->baseVirt), buf->vma->pageSize/ns->blocksize);
			assert(iod);
			requests.push_back(buf);
			buf->vma->setClean(buf); // TODO: check if setClean should be before or after the write
		}
	}
	for(Buffer* buf: requests){
		while(buf->snapshotState.load() == BufferState::Writing){
			unvme_check_completion(ns, get_IOtoolkit().id);
			sizeWritten += buf->vma->pageSize;
		}
	}
	writeSize += sizeWritten;
}

void createCache(u64 physSize, int batch){
	uCacheManager = new uCache(physSize, batch);
	discover_ucache_files();
}
}

