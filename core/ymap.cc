#include <osv/ymap.hh>
#include <osv/trace.hh> 
#include <vector>
#include <thread>
#include <osv/cache.hh>
#include <bitset>

TRACEPOINT(trace_ymap_map, "");
TRACEPOINT(trace_ymap_map_ret, "");
TRACEPOINT(trace_ymap_steal, "");
TRACEPOINT(trace_ymap_steal_ret, "");
TRACEPOINT(trace_ymap_unmap, "");
TRACEPOINT(trace_ymap_unmap_ret, "");

u64 startPhysRegion = 0;
u64 sizePhysRegion = 0;
u64 aligned_start = 0;
u64 aligned_size = 0;

llfree_t* llfree_allocator;
static u64 alignement_huge_page = 2ull*1024*1024;

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
	aligned_start = ((startPhysRegion + alignement_huge_page-1) & ~(alignement_huge_page - 1));
	aligned_size = sizePhysRegion - (aligned_start - startPhysRegion);
	aligned_start = aligned_start;
	u64 nb_frames = aligned_size / 4096;
	std::cout << "og:      " << std::bitset<64>(startPhysRegion) << " -> " << std::bitset<64>(startPhysRegion+sizePhysRegion) << std::endl;
	std::cout << "aligned: " << std::bitset<64>(aligned_start) << " -> " << std::bitset<64>(aligned_start + aligned_size) << std::endl;
	llfree_allocator = llfree_setup(sched::cpus.size(), nb_frames, LLFREE_INIT_FREE);
	for(auto c: sched::cpus){
		auto *py = percpu_ymap.for_cpu(c);
		*py = new ymap(c->id);
	}
	printf("frames: %lu, including free: %lu\n", llfree_frames(llfree_allocator), llfree_free_frames(llfree_allocator));
}

// NOTE: this implementation assumes that these functions cannot get interrupted 
// and another thread being scheduled on the same core can call those functions
u64 ymap_getPage(int order){
	ymap& ymap = get_ymap();
	llfree_result_t res = llfree_get(llfree_allocator, ymap.id, llflags(order));
	assert(llfree_is_ok(res)); // TODO: change this to handle oom case
	//std::cout << "index " << std::bitset<64>(res.frame) << ", " << res.frame << std::endl;
	u64 phys = aligned_start + res.frame * 4096;
	//std::cout << "frame " << std::bitset<64>(phys) << std::endl;
	assert(phys >= aligned_start && phys < aligned_start + aligned_size);
	return phys>>12;
}

void ymap_putPage(u64 phys, int order){
	ymap& ymap = get_ymap();
	u64 index = ((phys<<12) - aligned_start)/4096;
	//printf("index: %lu\n", index);
	//std::cout << std::bitset<64>(phys<<12) << "\n" << std::bitset<64>(aligned_start) << std::endl;
	assert(index >= 0 && index < aligned_size / 4096);
	llfree_result_t res = llfree_put(llfree_allocator, ymap.id, index, llflags(order));
	assert(llfree_is_ok(res)); // TODO: error handling
}

bool ymap_tryMap(void* virtAddr, u64 phys){
	std::atomic<u64>* ptePtr = walkRef(virtAddr);
	PTE oldPTE = PTE(ptePtr->load());
  if(oldPTE.phys != 0){
		return false;
  }
	PTE pagePTE(oldPTE.word);
	pagePTE.present = 1;
	pagePTE.writable = 1;
	pagePTE.phys = phys;
	return ptePtr->compare_exchange_strong(oldPTE.word, pagePTE.word);
}

#pragma GCC push_options
#pragma GCC optimize ("O0")
void ymap_unmap(void* virt){
	std::atomic<u64>* ptePtr = walkRef(virt);
	PTE oldPTE = PTE(ptePtr->load());
	oldPTE.phys = 0;
	ptePtr->store(oldPTE.word);
}
#pragma GCC pop_options

u64 ymap_tryUnmap(void* virt){
	std::atomic<u64>* ptePtr = walkRef(virt);
	PTE oldPTE = PTE(ptePtr->load());
	if(oldPTE.present == 1){
		printf("should not be happening\n");
		assert(false);
	}
	if(oldPTE.phys == 0)
		return 0;
	u64 phys = oldPTE.phys;
	assert(phys!=0ull);
	PTE pagePTE(oldPTE.word);
	pagePTE.phys = 0;
	if(ptePtr->compare_exchange_strong(oldPTE.word, pagePTE.word))
		return phys;
	else
		return 0;
}
