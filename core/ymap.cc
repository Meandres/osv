#include <osv/ymap.hh>
#include <osv/trace.hh> 
#include <vector>
#include <thread>
#include <osv/cache.hh>

TRACEPOINT(trace_ymap_map, "");
TRACEPOINT(trace_ymap_map_ret, "");
TRACEPOINT(trace_ymap_steal, "");
TRACEPOINT(trace_ymap_steal_ret, "");
TRACEPOINT(trace_ymap_unmap, "");
TRACEPOINT(trace_ymap_unmap_ret, "");

u64 startPhysRegion = 0;
u64 sizePhysRegion = 0;

BundleList* fullList;
BundleList* emptyList;

struct ymap {
   PageBundle* currentBundle;
   ymap(){
      currentBundle = fullList->get();
   }
};

PERCPU(ymap*, percpu_ymap);

inline ymap& get_ymap(){
    return **percpu_ymap;
}

void initYmaps(){
	fullList = new BundleList(sizePhysRegion/(4096*512) + 10); // leave extra space
	emptyList = new BundleList(sizePhysRegion/(4096*512) + 10); // leave extra space
	u64 nbFrames = sizePhysRegion / 4096;
	for(u64 i = 0; i < nbFrames/512; i++){
		PageBundle* bundle = new PageBundle();
		for(u64 j = 0; j<512; j++){
			bundle->insert((startPhysRegion+(i*512+j)*pageSize) >> 12);
		}
		assert(bundle->index==0);
		fullList->put(bundle);
	}
	for(auto c: sched::cpus){
		auto *py = percpu_ymap.for_cpu(c);
		*py = new ymap();
	}
}

// NOTE: this implementation assumes that these functions cannot get interrupted 
// and another thread being scheduled on the same core can call those functions
u64 ymap_getPage(){
	ymap& ymap = get_ymap();
	if(ymap.currentBundle->index == 512){ // need to refill
		emptyList->put(ymap.currentBundle);
		ymap.currentBundle = fullList->get();
	}
	return ymap.currentBundle->retrieve();
}

void ymap_putPage(u64 phys){
	ymap& ymap = get_ymap();
	if(ymap.currentBundle->index == 0){ // full bundle
		fullList->put(ymap.currentBundle);
		ymap.currentBundle = emptyList->get();
	}
	ymap.currentBundle->insert(phys);
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

u64 ymap_tryUnmap(void* virt){
	std::atomic<u64>* ptePtr = walkRef(virt);
	PTE oldPTE = PTE(ptePtr->load());
	if(oldPTE.phys == 0)
		return 0;
	u64 phys = oldPTE.phys;
	assert(phys!=0ull);
	PTE pagePTE(oldPTE.word);
  pagePTE.user = 1;
	pagePTE.phys = 0;
	if(ptePtr->compare_exchange_strong(oldPTE.word, pagePTE.word))
		return phys;
	else
		return 0;
}
