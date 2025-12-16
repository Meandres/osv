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
    assert_crash(bs != NULL);
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

    if(bs->ptes[0].present == 0){ // misprediction
      ucache::uCacheManager->mispredictions++;
      vma->misprediction_callback(vma->getBuffer(baseVirt));
    }
    u64 phys = bs->ptes[0].phys;
    for(size_t i = 0; i < vma->nbPages; i++){
      PTE newPTE = PTE(bs->ptes[i].word);
      newPTE.present = 0;
      newPTE.prefetcher = 0;
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
      newPTE.prefetcher = sched::cpu::current()->id+1;
      if(!(pteRefs+i)->compare_exchange_strong(bs->ptes[i].word, newPTE.word)){
        return false;
      }
    }
    bs->state = BufferState::Reading;
    return true;
  }

  bool Buffer::CachedToEvicting(BufferSnapshot* bs){
    if(bs->state != BufferState::Cached && bs->state != BufferState::ReadyToInsert){
      return false;
    }
    if(!vma->pre_CachedToEvicting_callback(vma->getBuffer(baseVirt)))
      return false;
    PTE newPTE = PTE(bs->ptes[0].word);
    newPTE.evicting = 1;
    if(!pteRefs->compare_exchange_strong(bs->ptes[0].word, newPTE.word)){
      return false;
    }
    bs->state = BufferState::Evicting;
    return true;
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
    vma->post_ReadyToInsertToCached_callback(vma->getBuffer(baseVirt));
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

  VMA::VMA(u64 size, u64 page_size, ResidentSet* set, ufile* f, callbacks implems):
    size(size), file(f), pageSize(page_size), id(nextVMAid++), residentSet(set)
  {
    assert_crash(isSupportedPageSize(page_size));
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
    callback_implems.post_ReadyToInsertToCached_callback_implem = implems.post_ReadyToInsertToCached_callback_implem;
    callback_implems.misprediction_callback_implem = implems.misprediction_callback_implem;
    callback_implems.prefetch_pol = implems.prefetch_pol;
    callback_implems.evict_pol = implems.evict_pol;
  }

  uCache::uCache() : totalPhysSize(0), evict_batch(0){
    usedPhysSize = 0;
    readSize = 0;
    writeSize = 0;
    prefetchedSize = 0;
    pageFaults = 0;
    tlbFlush = 0;
    mispredictions = 0;
    poll_depth = 0;
    poll_depth_count = 0;

    fs = new ufs();
  }

  void uCache::init(u64 physSize, int batch){
    this->totalPhysSize = physSize;
    this->prefetch_batch = 32;
    this->evict_batch = batch;
  }

  uCache::~uCache(){};

  VMA* uCache::mmap(const char* name, u64 req_size, u64 pageSize, ufile* file){
    assert_crash(name != NULL);
    VMA* vma;
    for(auto p: vmas){
      vma = p.second;
      if(strcmp(vma->file->name, name) == 0){
        return vma;
      }
    }
    ufile* f = file;
    if(f == NULL){
      f = fs->open_ufile<local_ufile>(name, req_size);
      assert_crash(f->size > 0);
    }
    assert_crash(f != NULL);
    vma = new VMA(align_up(f->size, pageSize), pageSize, new HashTableResidentSet(uCacheManager->totalPhysSize/pageSize), f, default_callbacks);
    for(u64 i = 0; i < vma->size / vma->pageSize; i++){
      vma->buffers.push_back(new Buffer(vma->start+(i*vma->pageSize), vma->pageSize, vma));
    }
    vmas.insert({(u64)vma->start, vma});
    //if(debug){
    cout << "Added a vm_area @ " << vma->start << " of size: " << vma->file->size << ", with pageSize: " << vma->pageSize << ", for file: " << name << endl;
    //}
    this->fs->devices[0]->switch_to_poll_mode();
    return vma;
  }

  VMA* uCache::getVMA(void* addr){
    if(addr == NULL || vmas.empty()){
      return NULL;
    }
    for(auto& p: vmas){
      if(p.second->isValidPtr(addr)){
        return p.second;
      }
    }
    return NULL;
  }

  bool uCache::checkPipeline(Buffer* buffer, BufferSnapshot* bs){
    if(bs->state == BufferState::Inconsistent){
      do{
        buffer->updateSnapshot(bs);
      } while(bs->state == BufferState::Inconsistent);
      if(bs->state == BufferState::Cached)
        return true;
    }
    if(bs->state == BufferState::Reading){
      if(buffer->getPrefetcher()-1 == sched::cpu::current()->id){ // this core started the prefetching
        buffer->vma->file->poll(buffer->snap->reqs);
        delete buffer->snap;
        buffer->snap = NULL;
      }else{
        do{ // wait for the prefetcher to poll the completion of the IO request
          _mm_pause();
          buffer->updateSnapshot(bs);
        } while(bs->state == BufferState::Reading);
      }
      //buffer->updateSnapshot(bs);
    }
    if(bs->state == BufferState::ReadyToInsert){
      if(buffer->getPrefetcher()-1 == sched::cpu::current()->id){ // this core started the prefetching
        if(buffer->snap != NULL){
          delete buffer->snap;
          buffer->snap=NULL;
        }
        assert_crash(buffer->ReadyToInsertToCached(bs));
        prefetchedSize += buffer->vma->pageSize;
      }else{
        do{ // wait for the prefetcher to poll the completion of the IO request
          _mm_pause();
          buffer->updateSnapshot(bs);
        } while(bs->state == BufferState::ReadyToInsert);
      }
      return true;
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
  u64 *percore_evict_count = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_init = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_evict = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_evict_choose = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_evict_write = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_evict_tlb = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_evict_unmap = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_alloc = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_map = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_read = (u64*)calloc(sched::cpus.size(), sizeof(u64));
  u64 *percore_end = (u64*)calloc(sched::cpus.size(), sizeof(u64));

  void reset_stats(int i){
    percore_count[i] = 0;
    percore_evict_count[i] = 0;
    percore_init[i] = 0;
    percore_evict[i] = 0;
    percore_evict_choose[i] = 0;
    percore_evict_write[i] = 0;
    percore_evict_tlb[i] = 0;
    percore_evict_unmap[i] = 0;
    percore_alloc[i] = 0;
    percore_map[i] = 0;
    percore_read[i] = 0;
    percore_end[i] = 0;
  }

  void uCache::handleFault(VMA* vma, Buffer* buffer, bool newPage){
    u64 start=0,m1=0,m2=0,m3=0,m4=0,m5=0,end=0;
    if(debug)
      start = processor::rdtsc();
    std::vector<Buffer*> pl; // need those here for goto to work 
    BufferSnapshot bs(vma->nbPages);
    buffer->updateSnapshot(&bs);
    phys_addr phys;
    if(checkPipeline(buffer, &bs)){
      return;
    }

    if(debug)
      m1 = processor::rdtsc();
    vma->choosePrefetchingCandidates(buffer->baseVirt, pl);
    ensureFreePages(vma->pageSize *(1 + pl.size())); // make enough room for the page being faulted and the prefetched ones
    if(debug)
      m2 = processor::rdtsc();

    buffer->updateSnapshot(&bs);
    if(checkPipeline(buffer, &bs)){
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
      /*while(PTE(*(buffer->pteRefs+vma->nbPages-1)).present == 0){
        _mm_pause();
        }*/
    }
  }

  void print_stats(){
    if(debug){
      u64 init=0, evict=0, evict_choose=0, evict_write=0, evict_tlb=0, evict_unmap=0, alloc=0, map=0, read=0, end=0, evict_count=0, count=0;
      for(u64 i=0; i<sched::cpus.size(); i++){
        init += percore_init[i];
        evict += percore_evict[i];
        alloc += percore_alloc[i];
        map += percore_map[i];
        read += percore_read[i];
        end += percore_end[i];
        count += percore_count[i];
        evict_choose += percore_evict_choose[i];
        evict_write += percore_evict_write[i];
        evict_tlb += percore_evict_tlb[i];
        evict_unmap += percore_evict_unmap[i];
        evict_count += percore_evict_count[i];
      }
      printf("init: %.2f\nevict: %.2f\nevict_choose: %.2f\nevict_write: %.2f\nevict_tlb: %.2f\nevict_unmap: %.2f\nalloc: %.2f\nmap: %.2f\nread: %.2f\nend: %.2f\ncount: %lu\nevict_count: %lu\n", init/(count+0.0), evict/(count+0.0), evict_choose/(evict_count+0.0), evict_write/(evict_count+0.0), evict_tlb/(evict_count+0.0), evict_unmap/(evict_count+0.0), alloc/(count+0.0), map/(count+0.0), read/(count+0.0), end/(count+0.0), count, evict_count);
      u64 total = init + evict + alloc + map + read + end;
      printf("Ratio io: %.2f\n", (read+evict_write)/(total+0.0));
      printf("Ratio tlb: %.4f\n", (evict_tlb)/(total+0.0));
    }
  }

  void uCache::prefetch(VMA *vma, PrefetchList pl){
    if(pl.size() == 0){ return;}
    for(Buffer* buf: pl){
      BufferSnapshot* bs = new BufferSnapshot(vma->nbPages);
      buf->updateSnapshot(bs);
      u64 phys = frames_alloc_phys_addr(vma->pageSize);
      if(buf->UncachedToPrefetching(phys, bs)){ // this can fail if another thread already resolved the prefetched buffer concurrently
        buf->snap = bs;
        buf->snap->reqs = vma->file->aread(buf->baseVirt, (u64)buf->baseVirt-(u64)vma->start, vma->pageSize, false);
        readSize += vma->pageSize;
        usedPhysSize += vma->pageSize;
        vma->usedPhysSize += vma->pageSize;
        assert_crash(vma->residentSet->insert(buf));
      }else{
        frames_free_phys_addr(phys, vma->pageSize); // put back unused candidates
        delete bs;
      }
    }
    //printf("prefetched %lu buffers\n", prefetchedSize/vma->pageSize);
  }

  void default_transparent_eviction(VMA* vma, u64 nbToEvict, EvictList el){
    while (el.size() < nbToEvict && nbToEvict*vma->pageSize < vma->usedPhysSize) {
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

  /*static const int nbCandidates = 5;
    void uCache::getVMACandidates(std::vector<VMACandidate*> *vmaCandidates){
    std::priority_queue<RegionWithSize, std::vector<RegionWithSize>, decltype(comp)> pq(comp);
    for(const VMA* c: vmas){
    vmaCandidates[size] = new VMACandidate(c, evict_batch/nbCandidates);
    size++;
    }else{
    int i = nbCandidates-1;
    while(vma->usedPhysSize > vmaCandidates[i] && i >= 0){

    }
    }
    }
    }
    }
    */

void uCache::evict(){
  u64 start=0,m1=0,m2=0,m3=0,end=0;
  std::vector<Buffer*> toEvict;
  toEvict.reserve(evict_batch*1.5);

  if(debug)
    start = processor::rdtsc();
  // 0. find candidates
  VMA* candidate_vma = vmas.begin()->second;
  for(const auto& p: vmas){
    if(candidate_vma->usedPhysSize.load() < p.second->usedPhysSize.load()){
      candidate_vma = p.second;
    }
  }
  if(candidate_vma->usedPhysSize/candidate_vma->pageSize <= evict_batch){
    printf("vma: %p, %lu\n", candidate_vma, candidate_vma->usedPhysSize.load());
  }
  assert_crash(candidate_vma->usedPhysSize/candidate_vma->pageSize > evict_batch);
  candidate_vma->chooseEvictionCandidates(evict_batch, toEvict);
  if(debug)
    m1 = processor::rdtsc();

  // write single pages that are dirty.
  flushBuffers(toEvict);

  if(debug)
    m2 = processor::rdtsc();

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

  if(debug)
    m3 = processor::rdtsc();

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
  if(debug){
    end = processor::rdtsc();
    percore_evict_choose[sched::cpu::current()->id] += (m1 - start);
    percore_evict_write[sched::cpu::current()->id] += (m2 - m1);
    percore_evict_tlb[sched::cpu::current()->id] += (m3 - m2);
    percore_evict_unmap[sched::cpu::current()->id] += (end - m3);
    percore_evict_count[sched::cpu::current()->id]++;
  }
}

void uCache::ensureFreePages(u64 additionalSize) {
  if (usedPhysSize+additionalSize >= totalPhysSize*0.95)
    evict();
}

void uCache::readBuffer(Buffer* buf){
  assert_crash(buf->vma != NULL);
  buf->vma->file->read(buf->baseVirt, (u64)buf->baseVirt-(u64)buf->vma->start, buf->vma->pageSize);
  readSize += buf->vma->pageSize;
}

void uCache::flushBuffers(std::vector<Buffer*>& toWrite){
  std::vector<Buffer*> requests;
  std::vector<int> devices;
  u64 sizeWritten = 0;
  requests.reserve(toWrite.size());
  /*bool ring_doorbell = false;
    if(!batch_io_request)
    ring_doorbell = true;*/
  for(u64 i=0; i<toWrite.size(); i++){
    Buffer* buf = toWrite[i];
    buf->updateSnapshot(buf->snap);
    if(buf->vma->isDirty(buf)){ // this writes the whole buffer
      assert_crash(buf->setIO());
      // best case, the last buffer is dirty and we can ring the doorbell directly
      // note that this is only possible when there is one device, if not then we have to ring each of the doorbells at the end
      //if(i == toWrite.size()-1) { ring_doorbell = true; } 
      buf->snap->reqs = buf->vma->file->awrite(buf->baseVirt, (u64)buf->baseVirt - (u64)buf->vma->start, buf->vma->pageSize, false);
      assert_crash(buf->snap->reqs);
      requests.push_back(buf);
    }
  }
  /*if(!ring_doorbell) // if the last buffer was clean need to manually ring the doorbell
    requests[0]->vma->file->commit_io();*/
  for(Buffer *p: requests){
    p->vma->file->poll(p->snap->reqs);
    delete p->snap->reqs;
    p->clearIO(true);
    sizeWritten += p->vma->pageSize;
  }
  writeSize += sizeWritten;
}

void createCache(u64 physSize, int batch){
  uCacheManager->init(physSize, batch);
  default_callbacks.isDirty_implem = pte_isDirty;
  default_callbacks.clearDirty_implem = pte_clearDirty;
  default_callbacks.setDirty_implem = empty_unconditional_callback;
  default_callbacks.canBeEvicted_implem = pte_canBeEvicted;
  default_callbacks.post_EvictingToUncached_callback_implem = empty_unconditional_callback;
  default_callbacks.pre_CachedToEvicting_callback_implem = empty_conditional_callback;
  default_callbacks.post_EvictingToCached_callback_implem = empty_unconditional_callback;
  default_callbacks.post_ReadyToInsertToCached_callback_implem = empty_unconditional_callback;
  default_callbacks.misprediction_callback_implem = empty_unconditional_callback;
  default_callbacks.prefetch_pol = default_prefetch;
  default_callbacks.evict_pol = default_transparent_eviction;
}

void initFile(const char* name, size_t size){
  return;
  struct file* filep;
  int fd = open(name, O_RDONLY);
  fget(fd, &filep);
  assert_crash(filep != NULL);
  struct stat stats;
  filep->stat(&stats);
  if(size <= (size_t)stats.st_size){
    close(fd);
    return;
  }
  close(fd);
  FILE* fp = fopen(name, "wb");
  if(fp == NULL){
    perror("Error");
  }
  assert_crash(fp != NULL);
  size_t block_size = 512;
  char* buf = (char*)malloc(block_size);
  memset(buf, 0, block_size);
  size_t total = size/block_size;
  assert_crash(size%block_size == 0);
  for(size_t i=0; i<total; i++){
    pwrite(fileno(fp), &buf, block_size, i*block_size);
  }
  fclose(fp);
}
};
