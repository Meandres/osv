#include "drivers/ymap.hh"

//u64 YmapRegionStartAddr=0;
//u64 YmapRegionSize=0;
__thread u64 pageStolen_parts = 0;
u64 pageStolen_aggregate = 0;

Ymap::Ymap(u64 count, int tid, void* virt) {
	//mmap_and_pmap(count);
	nbPagesToSteal = 100; // should it be dynamic and/or depend on cout
	interfaceId = tid;
	vec_lock.clear();
	list.reserve(count*1.5);
	for(u64 i=0; i<count; i++){
		memset(virt+i*pageSize, 0, pageSize);
		list.push_back(walk(virt+i*pageSize).phys);
	}
}

void Ymap::lock(){ // spinlock
	while(vec_lock.test_and_set(std::memory_order_acquire)){
		_mm_pause();
	}
}

void Ymap::unlock(){
	vec_lock.clear(std::memory_order_release);
}

void Ymap::setPhysAddr(void* virt, u64 phys){
	//PTE pagePTE(~0ull);
	PTE pagePTE(0ull);
	std::atomic<u64>* ptePtr = walkRef(virt);
	u64 oldPhys = PTE(ptePtr->load()).phys;
    if(oldPhys != 0){
        printf("%p -> %llu\n", virt, oldPhys);
    }
	assert(oldPhys == 0);
	pagePTE.present = 1;
	pagePTE.writable = 1;
	pagePTE.phys = phys;
	ptePtr->store(pagePTE.word);
}

u64 Ymap::clearPhysAddr(void* virt){
	std::atomic<u64>* ptePtr = walkRef(virt);
	u64 phys = PTE(ptePtr->load()).phys;
	assert(phys!=0ull);
	ptePtr->store(0ull);
	return phys;
}

YmapBundle::YmapBundle(u64 pageCount, int n_threads){
	void* virt = mmap(NULL, pageCount * pageSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	madvise(virt, pageCount * pageSize, MADV_NOHUGEPAGE);
	u64 pagesPerThread = pageCount / n_threads;
	for(int i=0;i<n_threads;i++){
		ymapInterfaces.push_back(new Ymap(pagesPerThread, i, virt + i*pagesPerThread*pageSize));
	}
}

bool YmapBundle::stealPages(int tid){
    //return false;
	int target = (ymapInterfaces[tid]->interfaceId+1)%ymapInterfaces.size();
	int toSteal = ymapInterfaces[tid]->nbPagesToSteal;
	u64 phys;
	while (toSteal > 0 && target != ymapInterfaces[tid]->interfaceId){
		if(ymapInterfaces[target]->currentlyStealing.load() == true){
			target = (target+1)%ymapInterfaces.size();
			continue;
		}
		do{
			phys = getPage(target, false);
			if(phys != 0){
				ymapInterfaces[tid]->list.push_back(phys);
				toSteal--;
			}
		}while(phys!=0 && toSteal>0);
		target = (target + 1)%ymapInterfaces.size();
	}
	if(toSteal==0){
        pageStolen_parts+=ymapInterfaces[tid]->nbPagesToSteal;
		return true;
    }
	return false;
}

u64 YmapBundle::getPage(int tid, bool canSteal=true){
	u64 phys;
	ymapInterfaces[tid]->lock();
	if(ymapInterfaces[tid]->list.size() == 0){
		if(!canSteal){ // fast exit
			ymapInterfaces[tid]->unlock();
			return 0;
		}else{
			assert(!ymapInterfaces[tid]->currentlyStealing.exchange(true, std::memory_order_acquire));
			int ret = stealPages(tid);
			assert(ret);
			assert(ymapInterfaces[tid]->currentlyStealing.exchange(false, std::memory_order_release));
		}
	}
	phys = ymapInterfaces[tid]->list.back();
	ymapInterfaces[tid]->list.pop_back();
	ymapInterfaces[tid]->unlock();
	return phys;
}

void YmapBundle::putPage(int tid, u64 phys){
	ymapInterfaces[tid]->list.push_back(phys);
}

elapsed_time YmapBundle::mapPhysPage(int tid, void* virtAddr){
    std::chrono::time_point<osv::clock::uptime> start;
    //u64 start;
    if(debugTime){
        start = osv::clock::uptime::now();
        //start = rdtsc();
    }
	u64 phys = getPage(tid);
	assert(phys!=0);
	ymapInterfaces[tid]->lock();
	ymapInterfaces[tid]->setPhysAddr(virtAddr, phys);
	ymapInterfaces[tid]->unlock();
    if(debugTime){
        return osv::clock::uptime::now() - start;
        //return rdtsc() - start;
    }

    return std::chrono::duration<int64_t, std::ratio<1, 1000000000>>();
    //return 0ull;
}

elapsed_time YmapBundle::unmapPhysPage(int tid, void* virtAddr){
    std::chrono::time_point<osv::clock::uptime> start;
    //u64 start;
    if(debugTime){
        start = osv::clock::uptime::now();
        //start = rdtsc();
    }
	ymapInterfaces[tid]->lock();
	u64 physAddr = ymapInterfaces[tid]->clearPhysAddr(virtAddr);
	putPage(tid, physAddr);
    assert(walk(virtAddr).word == 0ull);
	ymapInterfaces[tid]->unlock();
    if(debugTime){
        return osv::clock::uptime::now() - start;
        //return rdtsc() - start;
    }

    return std::chrono::duration<int64_t, std::ratio<1, 1000000000>>();
    //return 0ull;
}

void YmapBundle::unmapBatch(int tid, void* virtMem, std::vector<PID> toEvict){
	for(auto pid: toEvict){
		unmapPhysPage(tid, virtMem + pid);
	}
	//mmu::flush_tlb_all();
}


