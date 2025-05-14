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

std::atomic<u64> mmioAccesses = 0;
callbacks default_callbacks;

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


ucache_fs::ucache_fs(){
	lb_per_stripe = DEFAULT_LB_PER_STRIPE;
	scanned_for_files = false;
}

void ucache_fs::discover_ucache_files(){
		scanned_for_files = true;
		std::ifstream files("/nvme_files.txt");
		assert_crash(files.is_open());
		std::string name;
		u64 s_vlba, size, n_lb;
		std::string initial_text = "Available files on the ssd:\n";
		bool printed_initial=false;
		assert_crash(lb_per_stripe != 0);
		while(files >> name >> s_vlba >> size >> n_lb){
				if(!printed_initial)
						std::cout << initial_text;
				ucache_file* file = new ucache_file(name, s_vlba, n_lb, size);
				std::cout << name << ": from " << s_vlba << ", size: " << size << std::endl;
				available_files.push_back(file);
				printed_initial = true;
		}
		if(!printed_initial){
				printf("No files available on the SSD\n");
		}
}

void ucache_fs::add_device(const unvme_ns_t* ns){
		devices.push_back(ns);
		last_available_vlba += ns->blockcount;
}

ucache_file* ucache_fs::find_file(const char* name){
		if(!scanned_for_files){
			discover_ucache_files();
		}
		for(struct ucache_file* file: available_files){
				if(strcmp(file->name.c_str(), name) == 0){
						return file;
				}
		}
		return NULL;
}

u64 ucache_fs::findSpace(u64 nb_lb){
		if(available_files.empty()){
				return 0;
		}
		u64 end_previous_file = 0;
		for(ucache_file* f: available_files){
				if(f->start_vlba - end_previous_file > nb_lb){
						return end_previous_file;
				}
				end_previous_file = f->start_vlba+f->num_lb;
		}
		// couldn't find before any of the files, look after the last one.
		ucache_file* f = available_files.back();
		if(last_available_vlba - f->start_vlba+f->num_lb > nb_lb){
				return f->start_vlba + f->num_lb;
		}
		assert_crash(false); // out of space
		return 0; // unreachable
}

ucache_file* ucache_fs::create_file(u64 size){
		u64 num_lb = align_up(size/lb_size, lb_size);
		u64 start_vlba = findSpace(num_lb);
		ucache_file* f = new ucache_file("tmp", start_vlba, num_lb, size);
		return f;
}

void ucache_fs::computeStorageLocation(StorageLocation& loc, ucache_file* f, void* addr, void* start){
		u64 v_lba = f->start_vlba + reinterpret_cast<u64>((uintptr_t)addr - (uintptr_t)start)/lb_size;
		loc.device_id = (v_lba / lb_per_stripe) % devices.size();
		loc.stripe = v_lba / (devices.size() * lb_per_stripe);
		loc.offset_in_stripe = v_lba % lb_per_stripe;
}

void ucache_fs::read(ucache_file* f, void* addr, void* start, u64 size){
		StorageLocation loc;
		computeStorageLocation(loc, f, addr, start);
		int ret = unvme_read(devices[loc.device_id], sched::cpu::current()->id, addr, loc.stripe*lb_per_stripe + loc.offset_in_stripe, size/lb_size);
		assert_crash(ret == 0);
}

unvme_iod_t ucache_fs::aread(ucache_file* f, void* addr, void* start, u64 size){
		StorageLocation loc;
		computeStorageLocation(loc, f, addr, start);
		return unvme_aread(devices[loc.device_id], sched::cpu::current()->id, addr, loc.stripe*lb_per_stripe + loc.offset_in_stripe, size/lb_size);
}

unvme_iod_t ucache_fs::awrite(ucache_file* f, void* addr, void* start, u64 size, bool ring, std::vector<int>& devs){
		StorageLocation loc;
		computeStorageLocation(loc, f, addr, start);
		bool insert = true;
		for(int i: devs){ if((u64)i==loc.device_id) { insert = false; } }
		if(insert){	devs.push_back(loc.device_id); }
		return unvme_awrite(devices[loc.device_id], sched::cpu::current()->id, addr, loc.stripe*lb_per_stripe + loc.offset_in_stripe, size/lb_size, ring);
}

void ucache_fs::poll(unvme_iod_t iod, bool writing){
		assert_crash(unvme_apoll(iod, UNVME_SHORT_TIMEOUT, writing) == 0);
}

void ucache_fs::commit_io(std::vector<int> &devs){
	for(int i: devs){
		unvme_ring_sq_doorbell(devices[i], sched::cpu::current()->id);
	}
}

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
											assert_crash(*l0 == emptypte.word);
										}
								}
							} 
					}
			}
	}
}

Buffer::Buffer(void* addr, u64 size, VMA* vma_ptr): baseVirt(addr), vma(vma_ptr){
	u64 workingPageSize = mmu::page_size;
	size_t nb = size / workingPageSize;
	pteRefs = walkRef(addr);
	for(size_t i = 0; i < nb; i++){
		std::atomic<u64> *ref = pteRefs+i;
		assert(ref->load() != 0);
	}
	this->snap = NULL;
}

static BufferState computePTEState(PTE pte){
    if(pte.inserting == 1 && pte.evicting == 0 && pte.io == 0){
        return BufferState::Inserting;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 1 && pte.present == 0 && pte.phys != 0){
        return BufferState::Reading;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 0 && pte.phys != 0){
        return BufferState::ReadyToInsert;
    }
    if(pte.inserting == 0 && pte.evicting == 1 && pte.io == 0){
        return BufferState::Evicting;
    }
    if(pte.inserting == 0 && pte.evicting == 1 && pte.io == 1){
        return BufferState::Writing;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 1 && pte.phys != 0){
        return BufferState::Cached;
    }
    if(pte.inserting == 0 && pte.evicting == 0 && pte.io == 0 && pte.present == 0 && pte.phys == 0){
        return BufferState::Uncached;
    }
    return BufferState::Inconsistent;
}

void Buffer::updateSnapshot(BufferSnapshot* bs){
	bs->state = BufferState::TBD;
	assert_crash(vma != NULL);
  for(size_t i = 0; i < vma->nbPages; i++){
  	bs->ptes[i] = PTE(*(pteRefs+i));
    BufferState w = computePTEState(bs->ptes[i]);
    if(bs->state == BufferState::TBD){
    	bs->state = w;
    }else{
    	if(w != bs->state && bs->state != BufferState::Inserting && bs->state != BufferState::Reading && bs->state != BufferState::ReadyToInsert && bs->state != BufferState::Evicting && bs->state != BufferState::Writing){
      	bs->state = BufferState::Inconsistent;
        return;
      }
    }
  }
}

void Buffer::tryClearAccessed(BufferSnapshot* bs){
	for(size_t i = 0; i < vma->nbPages; i++){ // just try to 
		PTE pte = bs->ptes[i];
		if(pte.accessed == 0){ // simply skip
			continue;
		}
		PTE newPTE = PTE(pte.word);
		newPTE.accessed = 0; 
		(pteRefs+i)->compare_exchange_strong(pte.word, newPTE.word); // best effort 
	}
}

int Buffer::getAccessed(BufferSnapshot *bs){
	int acc = 0;
	for(size_t i=0; i<vma->nbPages; i++){
		acc += bs->ptes[i].accessed;
	}
	return acc;
}

bool Buffer::UncachedToInserting(u64 phys, BufferSnapshot* bs){
	if(bs->state != BufferState::Uncached)
		return false;
	
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.present = 0;
		newPTE.phys = phys+i;
		if(i==0)
			newPTE.inserting = 1;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return false;
		}
		bs->ptes[i] = newPTE;	
	}
	bs->state = BufferState::Inserting;
	return true;
}

u64 Buffer::EvictingToUncached(BufferSnapshot* bs){
	if(bs->state != BufferState::Evicting)
		return 0;

	u64 phys = bs->ptes[0].phys;
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.present = 0;
		newPTE.evicting = 0;
		newPTE.phys = 0;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return 0;
		}
	}
	vma->post_EvictingToUncached_callback(vma->getBuffer(baseVirt));
	return phys;
}
/*
void Buffer::map(u64 phys){
	for(size_t i=0; i<vma->nbPages; i++){
		PTE newPTE = ptes[i];
		newPTE.present = 1;
		newPTE.phys = phys+i;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
}

u64 Buffer::unmap(){
	u64 phys = ptes[0].phys;
	for(size_t i=0; i<vma->nbPages; i++){
		PTE newPTE = PTE(ptes[i].word);
		newPTE.present = 0;
		newPTE.phys = 0;
		std::atomic<u64>* ref = pteRefs[i];
		ref->store(newPTE.word);
	}
	return phys;
}
*/
void Buffer::invalidateTLBEntries(){
	for(size_t i=0; i<vma->nbPages; i++){
		//u64 workingSize = huge ? sizeHugePage : sizeSmallPage;
		invalidateTLBEntry(baseVirt+i*mmu::page_size);
	}
}

bool Buffer::UncachedToPrefetching(u64 phys, BufferSnapshot* bs){
	if(bs->state != BufferState::Uncached){
		return false;
	}
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.present = 0;
		newPTE.io = 1;
		newPTE.phys = phys+i;
		newPTE.prefetcher = sched::cpu::current()->id;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return false;
		}
	}
	return true;
}

bool Buffer::CachedToEvicting(BufferSnapshot* bs){
	if(bs->state != BufferState::Cached){
		return false;
	}
	if(!vma->pre_CachedToEvicting_callback(vma->getBuffer(baseVirt)))
		return false;
	PTE newPTE = PTE(bs->ptes[0].word);
	newPTE.evicting = 1;
	return pteRefs->compare_exchange_strong(bs->ptes[0].word, newPTE.word);
}

bool Buffer::InsertingToCached(BufferSnapshot* bs){
	if(bs->state != BufferState::Inserting){
		return false;
	}
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.inserting = 0;
		newPTE.present = 1;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return false;
		}
		bs->ptes[i] = newPTE;
	}
	bs->state = BufferState::Cached;
	return true;
}

bool Buffer::EvictingToCached(BufferSnapshot* bs){
	if(bs->state != BufferState::Evicting){
		return false;
	}
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.evicting = 0;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return false;
		}
		bs->ptes[i] = newPTE;
	}
	vma->post_EvictingToCached_callback(vma->getBuffer(baseVirt));
	bs->state = BufferState::Cached;
	return true;
}

bool Buffer::ReadyToInsertToCached(BufferSnapshot* bs){
	if(bs->state != BufferState::ReadyToInsert){
		return false;
	}
	for(size_t i = 0; i < vma->nbPages; i++){
		PTE newPTE = PTE(bs->ptes[i].word);
		newPTE.present = 1;
		if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
			return false;
		}
		bs->ptes[i] = newPTE;
	}
	bs->state = BufferState::Cached;
	return true;
}

bool Buffer::setIO(){
	PTE newPTE = PTE(*pteRefs);
	newPTE.io = 1;
	(*pteRefs) |= newPTE.word;
	return true;
}

bool Buffer::clearIO(bool dirty){
	PTE newPTE = PTE(*pteRefs);
	newPTE.io = 0;
	(*pteRefs) &= newPTE.word;
	if(dirty)
		vma->clearDirty(vma->getBuffer(baseVirt));
	return true;
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

HashTableResidentSet::HashTableResidentSet(u64 maxCount){
	count = next_pow2(maxCount * 1.5);
	mask = count-1;
	clockPos = 0;
	ht = (Entry*)mmap(NULL, count * sizeof(Entry), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	madvise((void*)ht, count * sizeof(Entry), MADV_HUGEPAGE);
	for(u64 i = 0; i<count; i++){
		ht[i].buf.store((Buffer*)empty);
	}
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
		//assert_crash(buf != NULL && buf->snap == NULL);
		u64 pos = hash((uintptr_t)buf) & mask;
  	while (true) {
        Buffer* curr = ht[pos].buf.load();
        if(curr == buf){
            return false;
        }
        assert_crash(curr != buf);
        if (((uintptr_t)curr == empty) || ((uintptr_t)curr == tombstone)){
            if (ht[pos].buf.compare_exchange_strong(curr, buf)){
                return true;
						}
				}
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
        assert_crash(curr != buf);
        if (((uintptr_t)curr == empty) || ((uintptr_t)curr == tombstone))
            return false;
        pos = (pos + 1) & mask;
    }
}

bool HashTableResidentSet::remove(Buffer* buf) {
	assert_crash(buf->snap == NULL);
	u64 pos = hash((uintptr_t)buf) & mask;
    while (true) {
    	Buffer* curr = ht[pos].buf.load();
        if ((uintptr_t)curr == empty)
        	return false;

        if (curr == buf){
        	if (ht[pos].buf.compare_exchange_strong(curr, (Buffer*)tombstone)){
            	return true;
					}
				}
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

Buffer* HashTableResidentSet::getEntry(int i){
	Buffer* curr = ht[i].buf.load();
	if((curr != (Buffer*)tombstone) && (curr != (Buffer*)empty)){
		return curr;
	}
	return NULL;
}

static u64 nextVMAid = 1; // 0 is reserved for other vmas

VMA::VMA(u64 size, u64 page_size, ResidentSet* set, struct ucache_file* f, callbacks implems):
	size(size), file(f), pageSize(page_size), id(nextVMAid++), residentSet(set)
{
	assert_crash(isSupportedPageSize(page_size));
	assert_crash(page_size <= uCacheManager->fs->lb_per_stripe*uCacheManager->fs->lb_size); // check that one page fits on a single stripe
	bool huge = page_size == 2ul*1024*1024 ? true : false;
	start = createVMA(id, size, page_size, false, huge);
	nbPages = page_size/mmu::page_size;
	buffers.clear();
	callback_implems.isDirty_implem = implems.isDirty_implem;
	callback_implems.clearDirty_implem = implems.clearDirty_implem;
	callback_implems.setDirty_implem = implems.setDirty_implem;
	callback_implems.canBeEvicted_implem = implems.canBeEvicted_implem;
	callback_implems.post_EvictingToUncached_callback_implem = implems.post_EvictingToUncached_callback_implem;
	callback_implems.pre_CachedToEvicting_callback_implem = implems.pre_CachedToEvicting_callback_implem;
	callback_implems.post_EvictingToCached_callback_implem = implems.post_EvictingToCached_callback_implem;
	callback_implems.prefetch_pol = implems.prefetch_pol;
	callback_implems.evict_pol = implems.evict_pol;
}

VMA* VMA::newVMA(u64 size, u64 page_size)
{
    struct ucache_file* f = uCacheManager->fs->create_file(size);
		VMA* vma = new VMA(size, page_size, new HashTableResidentSet(uCacheManager->totalPhysSize/page_size), f, default_callbacks);
		for(u64 i = 0; i < vma->size / vma->pageSize; i++){
			vma->buffers.push_back(new Buffer(vma->start+(i*vma->pageSize), vma->pageSize, vma));
		}
		return vma;
}

VMA* VMA::newVMA(const char* name, u64 page_size)
{
	assert_crash(name != NULL);
	struct ucache_file* f = uCacheManager->fs->find_file(name);
	VMA* vma = new VMA(align_up(f->size, page_size), page_size, new HashTableResidentSet(uCacheManager->totalPhysSize/page_size), f, default_callbacks);
	for(u64 i = 0; i < vma->size / vma->pageSize; i++){
		vma->buffers.push_back(new Buffer(vma->start+(i*vma->pageSize), vma->pageSize, vma));
	}
	return vma;
}

uCache::uCache() : totalPhysSize(0), batch(0){
   	usedPhysSize = 0;
   	readSize = 0;
   	writeSize = 0;
		pageFaults = 0;
		tlbFlush = 0;

		fs = new ucache_fs();
}

void uCache::init(u64 physSize, int batch){
	this->totalPhysSize = physSize;
	this->batch = batch;
}

uCache::~uCache(){};

void* uCache::addVMA(u64 virtSize, u64 pageSize){
    VMA* vma = VMA::newVMA(virtSize, pageSize);
    vmas.insert({vma->id, vma});
    cout << "Added a vm_area @ " << vma->start << " of size: " << virtSize/(1024ull*1024*1024) << "GB, with pageSize: " << pageSize << endl;
		return vma->start;
}

VMA* uCache::getOrCreateVMA(const char* name, u64 pageSize){
	VMA* vma;
	for(auto p: vmas){
		vma = p.second;
		if(strcmp(vma->file->name.c_str(), name) == 0){
			return vma;
		}
	}
  vma = VMA::newVMA(name, pageSize);
  vmas.insert({vma->id, vma});
  cout << "Added a vm_area @ " << vma->start << " of size: " << vma->file->size << ", with pageSize: " << vma->pageSize << ", for file: " << name << endl;
	return vma;
}

VMA* uCache::getVMA(void* addr){
	for(auto &p: vmas){
		if(p.second->isValidPtr(addr)){
			return p.second;
		}
	}
	return NULL;
}

static bool checkPipeline(ucache_fs* fs, Buffer* buffer, BufferSnapshot* bs){
	if(bs->state == BufferState::Inconsistent){
		do{
			buffer->updateSnapshot(bs);
		} while(bs->state == BufferState::Inconsistent);
		if(bs->state == BufferState::Cached)
			return true;
	}	
	if(bs->state == BufferState::Reading){
		if(buffer->getPrefetcher() == sched::cpu::current()->id){ // this core started the prefettching
			fs->poll(buffer->snap->iod, false);
			delete buffer->snap;
			buffer->snap = NULL;
		}else{
			do{ // wait for the prefetcher to poll the completion of the IO request
				_mm_pause();
				buffer->updateSnapshot(bs);
			} while(bs->state == BufferState::Reading);
		}
	}

	if(bs->state == BufferState::ReadyToInsert){
		if(buffer->ReadyToInsertToCached(bs)){
			assert_crash(buffer->vma->residentSet->insert(buffer));
			return true;
		}
	}
	if(bs->state == BufferState::Cached){
			return true;
	}
	return false;
}

void uCache::handlePageFault(VMA* vma, void* faultingAddr, exception_frame *ef){
	pageFaults++;
	void* basePage = alignPage(faultingAddr, vma->pageSize);
	Buffer* buffer = vma->getBuffer(basePage);
	handleFault(vma, buffer);
}

u64 *percore_count = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_init = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_evict = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_alloc = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_map = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_read = (u64*)calloc(sched::cpus.size(), sizeof(u64));
u64 *percore_end = (u64*)calloc(sched::cpus.size(), sizeof(u64));

void uCache::handleFault(VMA* vma, Buffer* buffer, bool newPage){
	u64 start=0,m1=0,m2=0,m3=0,m4=0,m5=0,end=0;
	if(debug)
		start = processor::rdtsc();
	std::vector<Buffer*> pl; // need those here for goto to work 
	BufferSnapshot bs(vma->nbPages);
	buffer->updateSnapshot(&bs);
	phys_addr phys;
	if(checkPipeline(fs, buffer, &bs)){
		return;
	}
	
	if(debug)
		m1 = processor::rdtsc();
	pl.reserve(batch);
	vma->choosePrefetchingCandidates(buffer->baseVirt, pl);
	ensureFreePages(vma->pageSize *(1 + pl.size())); // make enough room for the page being faulted and the prefetched ones
	if(debug)
		m2 = processor::rdtsc();

	buffer->updateSnapshot(&bs);
	if(checkPipeline(fs, buffer, &bs)){
		return;
	}

	phys = frames_alloc_phys_addr(vma->pageSize);
	if(debug)
		m3 = processor::rdtsc();
	if(buffer->UncachedToInserting(phys, &bs)){
		if(debug)
			m4 = processor::rdtsc();
		if(!newPage) { readBuffer(buffer); }
		if(debug)
			m5 = processor::rdtsc();
		assert_crash(buffer->InsertingToCached(&bs));
		usedPhysSize += vma->pageSize;
		vma->usedPhysSize += vma->pageSize;
		assert_crash(vma->isValidPtr(buffer->baseVirt));
		assert_crash(vma->residentSet->insert(buffer));
		prefetch(vma, pl);
		if(debug){
			end = processor::rdtsc();
			percore_init[sched::cpu::current()->id] += (m1 - start);
			percore_evict[sched::cpu::current()->id] += (m2 - m1);
			percore_alloc[sched::cpu::current()->id] += (m3 - m2);
			percore_map[sched::cpu::current()->id] += (m4 - m3);
			percore_read[sched::cpu::current()->id] += (m5 - m4);
			percore_end[sched::cpu::current()->id] += (end - m5);
			percore_count[sched::cpu::current()->id]++;
		}
	}else{
		frames_free_phys_addr(phys, vma->pageSize);
		while(PTE(*(buffer->pteRefs+vma->nbPages-1)).present == 0){
			_mm_pause();
		}
	}
}

void print_stats(){
	if(debug){
		u64 init=0, evict=0, alloc=0, map=0, read=0, end=0, count=0;
		for(u64 i=0; i<sched::cpus.size(); i++){
			init += percore_init[i];
			evict += percore_evict[i];
			alloc += percore_alloc[i];
			map += percore_map[i];
			read += percore_read[i];
			end += percore_end[i];
			count += percore_count[i];	
		}
		printf("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lu\n", init/(count+0.0), evict/(count+0.0), alloc/(count+0.0), map/(count+0.0), read/(count+0.0), end/(count+0.0), count);
	}
}

void uCache::prefetch(VMA *vma, PrefetchList pl){
	u64 prefetchedSize = 0;
	for(Buffer* buf: pl){
		BufferSnapshot* bs = new BufferSnapshot(vma->nbPages);
		buf->updateSnapshot(bs);
		u64 phys = frames_alloc_phys_addr(vma->pageSize);
		if(buf->UncachedToPrefetching(phys, bs)){ // this can fail if another thread already resolved the prefetched buffer concurrently
			buf->snap = bs;
			buf->snap->iod = fs->aread(vma->file, buf->baseVirt, vma->start, vma->pageSize);
			usedPhysSize += vma->pageSize;
			prefetchedSize += vma->pageSize;
		}else{
			frames_free_phys_addr(phys, vma->pageSize); // put back unused candidates
			delete bs;
		}
	}
	vma->usedPhysSize += prefetchedSize;
}

void default_transparent_eviction(VMA* vma, u64 nbToEvict, EvictList el){
    while (el.size() < nbToEvict) {
			u64 stillToFind = nbToEvict - el.size();
			u64 initial = vma->residentSet->getNextBatch(stillToFind);
			for(u64 i = 0; i<stillToFind; i++){
				u64 index = (initial+i) & vma->residentSet->mask;
				Buffer* buffer = vma->residentSet->getEntry(index);
				if(buffer == NULL){
					continue;
				}
				BufferSnapshot* bs = new BufferSnapshot(vma->nbPages);
				buffer->updateSnapshot(bs);
    		if(buffer->getAccessed(bs) == 0){ // if not accessed since it was cleared
					if(!vma->addEvictionCandidate(buffer, bs, el)){
						delete bs;
					}
    		}else{ // accessed == 1
        	buffer->tryClearAccessed(bs); // don't care if it fails
					delete bs;
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
    assert_crash(toEvict.size() <= maxQueueSize);
    flush(toEvict);
    
    // checking if the page have been remapped only improve performance
    // we need to settle on which pages to flush from the TLB at some point anyway
    // since we need to batch TLB eviction
		std::vector<void*> addressesToFlush;
		addressesToFlush.reserve(toEvict.size());
		toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](Buffer* buf) {
				buf->updateSnapshot(buf->snap);
        if(!buf->vma->canBeEvicted(buf)){
						assert_crash(buf->EvictingToCached(buf->snap));
						delete buf->snap;
						buf->snap = NULL;
						assert_crash(buf->vma->isValidPtr(buf->baseVirt));
  					assert_crash(buf->vma->residentSet->insert(buf)); // return the page to the RS
            return true;
        }
				for(u64 i = 0; i < buf->vma->nbPages; i++){
					addressesToFlush.push_back(buf->baseVirt+i*mmu::page_size);
				}
				return false;
    }), toEvict.end());

    if(addressesToFlush.size() < mmu::invlpg_max_pages){
				mmu::invlpg_tlb_all(&addressesToFlush);
    }else{
        mmu::flush_tlb_all();
    }
		tlbFlush++;

    u64 actuallyEvictedSize = 0;
		std::map<VMA*, u64> evictedSizePerVMA;
    for(Buffer* buf: toEvict){
				buf->updateSnapshot(buf->snap);
				if(buf->vma->canBeEvicted(buf)){
            u64 phys = buf->EvictingToUncached(buf->snap);
            if(phys != 0){
                // after this point the page has completely left the cache and any access will trigger 
                // a whole new allocation
								VMA* vma = buf->vma;
								frames_free_phys_addr(phys, vma->pageSize); // put back unused candidates
                actuallyEvictedSize += vma->pageSize;
								delete buf->snap;
								buf->snap = NULL;
								if(evictedSizePerVMA.find(vma) == evictedSizePerVMA.end()){
									evictedSizePerVMA[vma] = vma->pageSize;
								}else{
									evictedSizePerVMA[vma] += vma->pageSize;
								}
            }else{
							delete buf->snap;
							buf->snap = NULL;
							assert_crash(buf->vma->isValidPtr(buf->baseVirt));
            	assert_crash(buf->vma->residentSet->insert(buf));
						}
        }else{
						delete buf->snap;
						buf->snap = NULL;
						assert_crash(buf->vma->isValidPtr(buf->baseVirt));
        		assert_crash(buf->vma->residentSet->insert(buf)); // return the page to the RS
				}
    }
		for(const auto& p: evictedSizePerVMA){
			VMA* vma = p.first;
			vma->usedPhysSize -= p.second; 
		}
    usedPhysSize -= actuallyEvictedSize;
		//printf("evicted: %lu\n", actuallyEvictedSize/mmu::page_size);
}

void uCache::ensureFreePages(u64 additionalSize) {
    if (usedPhysSize+additionalSize >= totalPhysSize*0.95)
        evict();
}

void uCache::readBuffer(Buffer* buf){
	assert_crash(buf->vma != NULL);
	fs->read(buf->vma->file, buf->baseVirt, buf->vma->start, buf->vma->pageSize);
	readSize += buf->vma->pageSize;
}

void uCache::flush(std::vector<Buffer*>& toWrite){
	std::vector<Buffer*> requests;
	std::vector<int> devices;
	u64 sizeWritten = 0;
	requests.reserve(toWrite.size());
	devices.reserve(fs->devices.size());
	bool ring_doorbell = false;
	if(batch_io_request)
		ring_doorbell = true;
	for(u64 i=0; i<toWrite.size(); i++){
		Buffer* buf = toWrite[i];
		buf->updateSnapshot(buf->snap);
		if(buf->vma->isDirty(buf)){ // this writes the whole buffer
			assert_crash(buf->setIO());
			// best case, the last buffer is dirty and we can ring the doorbell directly
			// note that this is only possible when there is one device, if not then we have to ring each of the doorbells at the end
			if(fs->devices.size() == 1 && i == toWrite.size()-1) { ring_doorbell = true; } 
			buf->snap->iod = fs->awrite(buf->vma->file, buf->baseVirt, buf->vma->start, buf->vma->pageSize, ring_doorbell, devices);
			assert_crash(buf->snap->iod);
			requests.push_back(buf);
		}
	}
	if(!ring_doorbell) // if the last buffer was clean need to manually ring the doorbell
		fs->commit_io(devices);
	for(Buffer *p: requests){
		fs->poll(p->snap->iod, true);
		sizeWritten += p->vma->pageSize;
	}
	writeSize += sizeWritten;
}

void createCache(u64 physSize, int batch){
	ucache_file::next_available_file_id = 0;
	uCacheManager->init(physSize, batch);
	default_callbacks.isDirty_implem = pte_isDirty;
	default_callbacks.clearDirty_implem = pte_clearDirty;
	default_callbacks.setDirty_implem = empty_unconditional_callback;
	default_callbacks.canBeEvicted_implem = pte_canBeEvicted;
	default_callbacks.post_EvictingToUncached_callback_implem = empty_unconditional_callback;
	default_callbacks.pre_CachedToEvicting_callback_implem = empty_conditional_callback;
	default_callbacks.post_EvictingToCached_callback_implem = empty_unconditional_callback;
	default_callbacks.prefetch_pol = default_prefetch;
	default_callbacks.evict_pol = default_transparent_eviction;
}
};

