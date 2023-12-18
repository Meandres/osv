#include "drivers/ymap.hh"

u64 YmapRegionStartAddr=0;
u64 YmapRegionSize=0;
std::vector<Ymap*>ymapInterfaces;

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
	PTE pagePTE(0ull);
	std::atomic<u64>* ptePtr = walkRef(virt);
	u64 oldPhys = PTE(ptePtr->load()).phys;
	assert(oldPhys == 0);
	pagePTE.present = 1;
	pagePTE.writable = 1;
	pagePTE.phys = phys;
	ptePtr->store(pagePTE.word);
}

u64 Ymap::clearPhysAddr(void* virt){
	//memset(virt, 0, pageSize);
	std::atomic<u64>* ptePtr = walkRef(virt);
	u64 phys = PTE(ptePtr->load()).phys;
	assert(phys!=0ull);
	ptePtr->store(0ull);
	invalidateTLBEntry(virt);
	return phys;
}

bool Ymap::stealPages(){
	int target = (interfaceId+1)%ymapInterfaces.size();
	int toSteal = nbPagesToSteal;
	//std::cout << "Stealing" << std::endl;
	u64 phys;
	while (toSteal > 0 && target != interfaceId){
		if(ymapInterfaces[target]->currentlyStealing.load() == true){
			target = (target+1)%ymapInterfaces.size();
			//std::cout << "Skipping interface already stealing" << std::endl;
			continue;
		}
		do{
			phys = ymapInterfaces[target]->getPage(false);
			if(phys != 0){
				list.push_back(phys);
				toSteal--;
			}
		}while(phys!=0 && toSteal>0);
		target = (target + 1)%ymapInterfaces.size();
	}
	//std::cout << "toSteal " <<toSteal << std::endl;
	if(toSteal==0)
		return true;
	return false;
}

u64 Ymap::getPage(bool canSteal=true){
	u64 phys;
	lock();
	/*if(mappingCount > 0){
		mappingCount--;
		phys = walk(initialMapping++).phys;
		this->unlock();
		return phys;
	}*/
	if(list.size() == 0){
		if(!canSteal){ // fast exit
			unlock();
			return 0;
		}else{
			assert(!currentlyStealing.exchange(true, std::memory_order_acquire));
			int ret = stealPages();
			assert(ret);
			assert(currentlyStealing.exchange(false, std::memory_order_release));
		}
	}
	phys = list.back();
	list.pop_back();
	unlock();
	return phys;
}

void Ymap::putPage(u64 phys){
	list.push_back(phys);
}

// get free page, return physical address
void Ymap::mapPhysPage(void* virtAddr){
	u64 phys = getPage();
	assert(phys!=0);
	lock();
	setPhysAddr(virtAddr, phys);
	unlock();
	//std::cout << "Mapped phys" << std::endl;
}

void Ymap::unmapPhysPage(void* virtAddr){
	lock();
	u64 physAddr = clearPhysAddr(virtAddr);
	putPage(physAddr);
	unlock();
}

void createYmapInterfaces(u64 pageCount, int n_threads){
	void* virt = mmap(NULL, pageCount * pageSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	madvise(virt, pageCount * pageSize, MADV_NOHUGEPAGE);
	u64 pagesPerThread = pageCount / n_threads;
	//std::cout << pagesPerThread << std::endl;
	//printf("%lX - %lX\n", virt+3*pagesPerThread*pageSize, virt+2*pagesPerThread*pageSize);
	for(int i=0;i<n_threads;i++){
		ymapInterfaces.push_back(new Ymap(pagesPerThread, i, virt + i*pagesPerThread*pageSize));
	}
	/*for(int i=1; i<n_threads; i++){
		assert(ymapInterfaces[i]->list.front()> ymapInterfaces[i-1]->list.back());
	}*/
	//std::cout << ymapInterfaces[0]->list.front() << " -- " << ymapInterfaces[ymapInterfaces.size()-1]->list.back() << std::endl;
}
