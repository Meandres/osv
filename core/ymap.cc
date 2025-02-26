#include <osv/ymap.hh>
#include <osv/trace.hh> 
#include <vector>
#include <thread>
#include <osv/cache.hh>
#include <bitset>

u64 startPhysRegion = 0;
u64 sizePhysRegion = 0;
u64 aligned_start = 0;
u64 aligned_size = 0;

llfree_t* llfree_allocator;

struct ymap {
	int id;
	ymap(int i){
		id = i;
	}
};

PERCPU(ymap*, percpu_ymap);

inline ymap& get_ymap(){
	return **percpu_ymap;
}

void initYmaps(){
	aligned_start = ((startPhysRegion + sizeHugePage-1) & ~(sizeHugePage - 1));
	aligned_size = sizePhysRegion - (aligned_start - startPhysRegion);
	aligned_start = aligned_start;
	u64 nb_frames = aligned_size / 4096;
	//std::cout << "og:      " << std::bitset<64>(startPhysRegion) << " -> " << std::bitset<64>(startPhysRegion+sizePhysRegion) << std::endl;
	//std::cout << "aligned: " << std::bitset<64>(aligned_start) << " -> " << std::bitset<64>(aligned_start + aligned_size) << std::endl;
	llfree_allocator = llfree_setup(sched::cpus.size(), nb_frames, LLFREE_INIT_FREE);
	for(auto c: sched::cpus){
		auto *py = percpu_ymap.for_cpu(c);
		*py = new ymap(c->id);
	}
	//printf("frames: %lu, including free: %lu\n", llfree_frames(llfree_allocator), llfree_free_frames(llfree_allocator));
}

// NOTE: this implementation assumes that these functions cannot get interrupted 
// and another thread being scheduled on the same core can call those functions
u64 ymap_getPage(int order){
	ymap& ymap = get_ymap();
	llfree_result_t res = llfree_get(llfree_allocator, ymap.id, llflags(order));
	assert(llfree_is_ok(res)); // TODO: change this to handle oom case
	u64 phys = aligned_start + res.frame * 4096;
	assert(phys >= aligned_start && phys < aligned_start + aligned_size);
	return phys>>12;
}

void ymap_putPage(u64 phys, int order){
	ymap& ymap = get_ymap();
	u64 index = ((phys<<12) - aligned_start)/4096;
	if(index < 0 || index >= aligned_size / 4096){
		printf("error %lu\n", index);
	}
	assert(index >= 0 && index < aligned_size / 4096);
	llfree_result_t res = llfree_put(llfree_allocator, ymap.id, index, llflags(order));
	assert(llfree_is_ok(res)); // TODO: error handling
}

Buffer::Buffer(void* addr, u64 size){
	baseVirt = addr;
	u64 workingPageSize = sizeSmallPage;
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
	assert(base.phys != 0); // evictors own the eviction so this cannot happen
	/*if(base.present == 1){
		printf("concurrent fault\n");
		return 0; // another core soft faulted the page
	}*/
	
	u64 phys = base.phys;
	PTE newPTE = PTE(base.word);
	newPTE.present = 0;
	newPTE.phys = 0;
	std::atomic<u64>* baseRef = pteRefs[0];
	if(baseRef->compare_exchange_strong(base.word, newPTE.word))
		return phys;
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
		invalidateTLBEntry(baseVirt+i*sizeSmallPage);
	}
}
