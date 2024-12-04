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

//Ymap::Ymap(u64 count, int tid, void* virt) {
Ymap::Ymap(u64 count, int tid){
	//mmap_and_pmap(count);
	nbPagesToSteal = 100; // should it be dynamic and/or depend on cout
	interfaceId = tid;
	vec_lock.clear();
    pageStolen = 0ULL;
    maxSize = count * 1.5;
    if(maxSize%8 != 0){
        maxSize -= (maxSize%8);
    }
    //list = (frameCol_t*)malloc(sizeof(frameCol_t)*maxSize/8);
    list.reserve(count*1.5);
    u64 maxAddr = startPhysRegion + count * pageSize * (tid+1); // stop at the last page of the region
	u64 handPos = startPhysRegion + count * pageSize * tid;
    //u64 frameAddr = handPos;
    /*for(u64 i = 0; i < count/8; i++){
        for(u64 cnt = 0; cnt < 8; cnt++){
            list[i].buf[cnt] = frameAddr>>12;
            frameAddr += 4096;
        }
    }*/
    for(u64 i=handPos; i<maxAddr; i+=4096){
        list.push_back(i>>12);
    }
    /*for(u64 i=0; i<count; i++){
		int* j = (int*)(virt+i*pageSize);
        *j=0;
		list.push_back(walk(j).phys);
	}*/
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
	PTE pagePTE(0ull);
	std::atomic<u64>* ptePtr = walkRef(virt);
	PTE oldPTE = PTE(ptePtr->load());
    if(oldPTE.phys != 0){
        printf("phys: %lu\n", oldPTE.phys);
        printf("state %lu\n", (get_mmr(virt)->getPageState(get_mmr(virt)->toPID(virt))).getState());
	    assert(oldPTE.phys == 0);
    }
	pagePTE.present = 1;
	pagePTE.writable = 1;
    pagePTE.user = oldPTE.user;
	pagePTE.phys = phys;
	ptePtr->store(pagePTE.word);
}

u64 Ymap::clearPhysAddr(void* virt){
	std::atomic<u64>* ptePtr = walkRef(virt);
	u64 phys = PTE(ptePtr->load()).phys;
	assert(phys!=0ull);
	PTE pagePTE(0ull);
    pagePTE.user = 1;
	ptePtr->store(pagePTE.word);
	return phys;
}

/*u64 Ymap::getPage(){
    u64 phys;
    lock();
    if(list.size() == 0){
        assert(!currentlyStealing.exchange(true, std::memory_order_acquire));
        assert(stealPages());
        assert(currentlyStealing.exchange(false, std::memory_order_release));
    }
    phys = list.back();
    list.pop_back();
    unlock();
    return phys;
}

void Ymap::putPage(u64 phys){
    lock();
    list.push_back(phys);
    unlock();
}*/

YmapBundle::YmapBundle(u64 pageCount, int n_threads){
    if(pageCount * pageSize > sizePhysRegion){
        assert(false);
    }
	u64 pagesPerThread = pageCount / n_threads;
    for(int i=0; i<n_threads; i++){
        ymapInterfaces.push_back(new Ymap(pagesPerThread, i));
    }
}

bool YmapBundle::stealPages(int tid){
    trace_ymap_steal();
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
                putPage(tid, phys);
				toSteal--;
			}
		}while(phys!=0 && toSteal>0);
		target = (target + 1)%ymapInterfaces.size();
	}
	if(toSteal==0){
        ymapInterfaces[tid]->pageStolen += ymapInterfaces[tid]->nbPagesToSteal;
        trace_ymap_steal_ret();
		return true;
    }
    trace_ymap_steal_ret();
	return false;
}

u64 YmapBundle::getPage(int tid, bool canSteal){
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

void YmapBundle::mapPhysPage(int tid, void* virtAddr){
    trace_ymap_map();
	u64 phys = getPage(tid, true);
	assert(phys!=0);
	ymapInterfaces[tid]->lock();
	ymapInterfaces[tid]->setPhysAddr(virtAddr, phys);
	ymapInterfaces[tid]->unlock();
    trace_ymap_map_ret();
}

void YmapBundle::unmapPhysPage(int tid, void* virtAddr){
    trace_ymap_unmap();
	ymapInterfaces[tid]->lock();
	u64 physAddr = ymapInterfaces[tid]->clearPhysAddr(virtAddr);
	putPage(tid, physAddr);
    assert(walk(virtAddr).phys == 0ull);
	ymapInterfaces[tid]->unlock();
    trace_ymap_unmap_ret();
}
    
bool YmapBundle::tryMap(int tid, void* virtAddr, u64 phys){
	std::atomic<u64>* ptePtr = walkRef(virtAddr);
	PTE oldPTE = PTE(ptePtr->load());
    if(oldPTE.phys != 0){
        return false;
    } 
	PTE pagePTE(0ull);
	pagePTE.present = 1;
	pagePTE.writable = 1;
    pagePTE.user = oldPTE.user;
	pagePTE.phys = phys;
	return ptePtr->compare_exchange_strong(oldPTE.word, pagePTE.word);
}

void YmapBundle::unmapBatch(int tid, void* virtMem, std::vector<PID> toEvict){
	for(auto pid: toEvict){
		unmapPhysPage(tid, virtMem + pid);
	}
	//mmu::flush_tlb_all();
}


